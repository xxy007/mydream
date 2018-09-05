package pipeline;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.Logger;
import configuration.StorageConf;
import crc32.MyCrc;
import datanode.storage.DataNodeStorage;
import exception.ChecksumException;
import exception.PipelineException;
import io.netty.util.internal.StringUtil;
import tools.ByteObject;
import tools.ExceptionUtil;

public class Pipeline {
	private LinkedList<DataPackage> ackQueue;
	private long blockId;
	private String preIp;
	private String nextIp;
	private int receiveDataPort;
	private int receiveResponsePort;
	private int sendDataPort = 0;
	private int sendResponsePort = 0;
	private boolean isLast;
	private boolean isRun = false;
	private Logger logger = Logger.getLogger(Pipeline.class);
	private static int TIMEOUT_MILLSECOND = 60000;

	public Pipeline(long blockId, boolean isLast) {
		super();
		ackQueue = new LinkedList<DataPackage>();
		this.blockId = blockId;
		this.isLast = isLast;
	}

	public void setPipelineInfo(String preIp, String nextIp, int sendDataPort, int sendResponsePort) {
		this.preIp = preIp;
		this.nextIp = nextIp;
		this.sendDataPort = sendDataPort;
		this.sendResponsePort = sendResponsePort;
	}

	public PortInfo setUpPipeline() {
		PortInfo portInfo = null;
		ServerSocket receiveDataServer = null;
		ServerSocket receiveResponseServer = null;
		try {
			receiveDataServer = new ServerSocket(0, 1);
			receiveDataPort = receiveDataServer.getLocalPort();
			DataRunnable dataRunnable = new DataRunnable();
			Thread dataThread = new Thread(dataRunnable);
			dataThread.setUncaughtExceptionHandler(new UncaughtPipelineException());
			dataThread.run();

			receiveResponseServer = new ServerSocket(0, 1);
			receiveResponsePort = receiveResponseServer.getLocalPort();
			ResponseRunnable responseRunnable = new ResponseRunnable();
			Thread responseThread = new Thread(responseRunnable);
			responseThread.setUncaughtExceptionHandler(new UncaughtPipelineException());
			responseThread.run();
			portInfo = new PortInfo(receiveDataPort, receiveResponsePort);
		} catch (IOException e) {
			logger.error(ExceptionUtil.getStackTrace(e));
			isRun = false;
			portInfo = null;
		} finally {
			try {
				closeObject(receiveResponseServer);
				closeObject(receiveDataServer);
			} catch (IOException e) {
				isRun = false;
				logger.error(ExceptionUtil.getStackTrace(e));
			}
		}
		return isRun ? portInfo : null;
	}

	private class DataRunnable implements Runnable {
		private String filePath;

		public DataRunnable() {
			super();
			String dataDir = StorageConf.getVal("dataNode.storage.dir");
			this.filePath = dataDir + File.separator + "bk_" + blockId;
		}

		@Override
		public void run() {
			if (!isLast) {
				runNotLast();
			} else {
				runLast();
			}
		}

		private void runLast() {
			ServerSocket server = null;
			Socket socket = null;
			ObjectInputStream input = null;
			OutputStream out = null;
			try {
				server = new ServerSocket(receiveDataPort);
				server.setSoTimeout(TIMEOUT_MILLSECOND);
				socket = server.accept();
				input = new ObjectInputStream(socket.getInputStream());
				out = new FileOutputStream(filePath);
				while (isRun) {
					DataPackage dataPackage = (DataPackage) input.readObject();
					int chunksumEnd = dataPackage.getChunksumEnd();
					int dataStart = dataPackage.getDataStart();
					int dataEnd = dataPackage.getDataEnd();
					byte[] packageBuf = dataPackage.getPackageBuf();
					if (dataStart == dataEnd) {
						break;
					}
					int i, j;
					for (i = 0, j = dataStart; i < chunksumEnd && j < dataEnd; i = i + 4, j = j + 512) {
						int chunksum = ByteObject.byteArrayToInt(packageBuf, i, 4);
						int crc32 = MyCrc.getCrc32(packageBuf, j, 512);
						if (chunksum != crc32) {
							throw new ChecksumException("获取到的数据的crc验证码和解析得到的crc验证码不一致，数据错误");
						}
						out.write(packageBuf, j, 512);
					}
					if (i == chunksumEnd && j == dataEnd) {

					} else if (i == chunksumEnd && j > dataEnd) {
						int chunksum = ByteObject.byteArrayToInt(packageBuf, i - 4, 4);
						int crc32 = MyCrc.getCrc32(packageBuf, j - 512, dataEnd);
						if (chunksum != crc32) {
							throw new ChecksumException("获取到的数据的crc验证码和解析得到的crc验证码不一致，数据错误");
						}
						out.write(packageBuf, j - 512, dataEnd);

					} else {
						throw new ChecksumException("获取到的数据的crc验证码和解析得到的crc验证码不一致，数据错误");
					}
					ackQueue.add(dataPackage);
					ackQueue.notifyAll();
				}
			} catch (IOException | ClassNotFoundException e) {
				logger.error(ExceptionUtil.getStackTrace(e));
				isRun = false;
			} finally {
				try {
					closeObject(out);
					closeObject(input);
					closeObject(socket);
					closeObject(server);
				} catch (IOException e) {
					isRun = false;
					logger.error(ExceptionUtil.getStackTrace(e));
				}
			}
		}

		private void runNotLast() {
			ServerSocket server = null;
			Socket socket = null;
			ObjectInputStream input = null;
			OutputStream out = null;
			Socket sendSocket = null;
			ObjectOutputStream sendOutput = null;
			try {
				server = new ServerSocket(receiveDataPort);
				server.setSoTimeout(TIMEOUT_MILLSECOND);
				socket = server.accept();
				input = new ObjectInputStream(socket.getInputStream());
				out = new FileOutputStream(filePath);
				checkIpAndPort(nextIp, sendDataPort);
				sendSocket = new Socket(nextIp, sendDataPort);
				sendOutput = new ObjectOutputStream(sendSocket.getOutputStream());
				while (isRun) {
					DataPackage dataPackage = (DataPackage) input.readObject();
					int dataStart = dataPackage.getDataStart();
					int dataEnd = dataPackage.getDataEnd();
					byte[] packageBuf = dataPackage.getPackageBuf();
					ackQueue.add(dataPackage);
					ackQueue.notifyAll();
					sendOutput.writeObject(dataPackage);
					if (dataStart == dataEnd) {
						break;
					}
					out.write(packageBuf, dataStart, dataEnd);
				}
			} catch (IOException | ClassNotFoundException e) {
				logger.error(ExceptionUtil.getStackTrace(e));
				isRun = false;
			} finally {
				try {
					closeObject(out);
					closeObject(input);
					closeObject(socket);
					closeObject(server);
					closeObject(sendOutput);
					closeObject(sendSocket);
				} catch (IOException e) {
					isRun = false;
					logger.error(ExceptionUtil.getStackTrace(e));
				}
			}
		}
	}

	private class ResponseRunnable implements Runnable {
		@Override
		public void run() {
			if (!isLast) {
				runNotLast();
			} else {
				runLast();
			}
		}
		private void runLast() {
			try (Socket respSocket = new Socket(preIp, sendResponsePort);
					ObjectOutputStream respOutput = new ObjectOutputStream(respSocket.getOutputStream());) {
				while (isRun) {
					DataPackage firstPackage;
					while ((firstPackage = ackQueue.getFirst()) == null) {
						ackQueue.wait();
					}
					ResponseInfo responseInfo = new ResponseInfo();
					responseInfo.setReqId(firstPackage.getReqId());
					responseInfo.setResultCode(100);
					respOutput.writeObject(responseInfo);
					if (firstPackage.getDataStart() == firstPackage.getDataEnd()) {
						ackQueue.clear();
						break;
					}
				}
			} catch (IOException | InterruptedException e) {
				logger.error(ExceptionUtil.getStackTrace(e));
				isRun = false;
			} finally {
			}
		}
		private void runNotLast() {
			ServerSocket receiveResponseServer = null;
			Socket receiveResponseSocket = null;
			ObjectInputStream receiveInput = null;
			Socket sendResponseSocket = null;
			ObjectOutputStream sendResponseOutput = null;
			try {
				receiveResponseServer = new ServerSocket(receiveResponsePort);
				receiveResponseServer.setSoTimeout(TIMEOUT_MILLSECOND);
				receiveResponseSocket = receiveResponseServer.accept();
				receiveInput = new ObjectInputStream(receiveResponseSocket.getInputStream());
				checkIpAndPort(preIp, sendResponsePort);
				sendResponseSocket = new Socket(preIp, sendResponsePort);
				sendResponseOutput = new ObjectOutputStream(sendResponseSocket.getOutputStream());
				while (isRun) {
					ResponseInfo responseInfo = (ResponseInfo) receiveInput.readObject();
					if (responseInfo.getResultCode() != 100) {
						sendResponseOutput.writeObject(responseInfo);
						break;
					}
					long reqId = responseInfo.getReqId();
					DataPackage firstPackage;
					while ((firstPackage = ackQueue.getFirst()) == null) {
						ackQueue.wait();
					}
					long ackReqId = firstPackage.getReqId();
					if (ackReqId != reqId) {
						responseInfo.setReason("获取到的应答数据和自身记录数据不一致，pipeline出错");
						responseInfo.setResultCode(110);
						sendResponseOutput.writeObject(responseInfo);
						break;
					}
					if (firstPackage.getDataStart() == firstPackage.getDataEnd()) {
						sendResponseOutput.writeObject(responseInfo);
						ackQueue.clear();
						break;
					}
					sendResponseOutput.writeObject(responseInfo);
				}
			} catch (IOException | ClassNotFoundException | InterruptedException e) {
				logger.error(ExceptionUtil.getStackTrace(e));
				isRun = false;
			} finally {
				try {
					closeObject(receiveInput);
					closeObject(receiveResponseSocket);
					closeObject(receiveResponseServer);
					closeObject(sendResponseOutput);
					closeObject(sendResponseSocket);
				} catch (IOException e) {
					isRun = false;
					logger.error(ExceptionUtil.getStackTrace(e));
				}
			}
		}
	}

	private class UncaughtPipelineException implements UncaughtExceptionHandler {
		public void uncaughtException(Thread a, Throwable e) {
			isRun = false;
			logger.error(ExceptionUtil.getStackTrace(e));
		}
	}

	private void closeObject(Object obj) throws IOException {
		if (obj != null && obj instanceof java.io.Closeable) {
			java.io.Closeable closeObj = (java.io.Closeable) obj;
			closeObj.close();
		}
	}
	
	private boolean checkIpAndPort(String ip, int port) throws PipelineException {
		long startTime = System.currentTimeMillis();
		while (true) {
			long stopTime = System.currentTimeMillis();
			if(StringUtil.isNullOrEmpty(nextIp) || sendDataPort == 0) {
				if ((stopTime - startTime) > 30000) {
					throw new PipelineException("获取pipeline属性失败");
				}	
			}else {
				return true;
			}
		}
	}

	public static void main(String[] args) throws IOException {
		ServerSocket server1 = new ServerSocket(0, 1);
		int port1 = server1.getLocalPort();
		System.out.println(port1);
		// ServerSocket server2 = new ServerSocket(0, 1);
		// int port2 = server2.getLocalPort();
		// System.out.println(port2);
	}
}
