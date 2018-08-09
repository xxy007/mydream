package pipeline;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import org.apache.log4j.Logger;
import configuration.StorageConf;
import crc32.MyCrc;
import exception.ChecksumException;
import tools.ByteObject;
import tools.ExceptionUtil;

public class Pipeline {
	private LinkedList<DataPackage> ackQueue;
	private long blockId;
	private String preIp;
	private String nextIp;
	private int dataPort;
	private int responsePort;
	private boolean isLast;
	private boolean isRun = false;
	private Logger logger = Logger.getLogger(Pipeline.class);
	public Pipeline(long blockId, String preIp, String nextIp, int dataPort, int responsePort, boolean isLast) {
		super();
		ackQueue = new LinkedList<DataPackage>();
		this.blockId = blockId;
		this.preIp = preIp;
		this.nextIp = nextIp;
		this.dataPort = dataPort;
		this.responsePort = responsePort;
		this.isLast = isLast;
	}

	public void setUpPipeline() {
		isRun = true;
		PipelineThread pipeline = new PipelineThread();
		Thread t = new Thread(pipeline);
		t.setUncaughtExceptionHandler(new UncaughtPipelineException());
		t.start();
	}

	private class PipelineThread implements Runnable {
		@Override
		public void run() {
			ReceiveThread receive = new ReceiveThread();
			FutureTask<Boolean> receiveFutureTask = new FutureTask<>(receive, true);
			receiveFutureTask.run();

			long startTime = System.currentTimeMillis();
			while (true) {
				long stopTime = System.currentTimeMillis();
				try {
					boolean isConnect = connect(preIp, responsePort);
					if (isConnect) {
						break;
					}
					if ((stopTime - startTime) > 30000) {
						throw new SocketException("连接超时");
					}
				} catch (SocketException e) {
					logger.error(ExceptionUtil.getStackTrace(e));
					isRun = false;
					return;
				}
			}

			ResponseThread response = new ResponseThread();
			FutureTask<Boolean> responseFutureTask = new FutureTask<>(response, true);
			responseFutureTask.run();

			try {
				receiveFutureTask.get();
				responseFutureTask.get();
			} catch (InterruptedException | ExecutionException e) {
				logger.error(ExceptionUtil.getStackTrace(e));
				isRun = false;
			}
		}

	}

	private class ReceiveThread implements Runnable {
		private String filePath;

		public ReceiveThread() {
			super();
			String dataDir = StorageConf.getVal("dataNode.storage.dir");
			this.filePath = dataDir + File.separator + "bk_" + blockId;
		}

		@Override
		public void run() {
			if (!isLast) {
				try (Socket sendSocket = new Socket(nextIp, dataPort);
						ObjectOutputStream sendOutput = new ObjectOutputStream(sendSocket.getOutputStream());
						ServerSocket server = new ServerSocket(dataPort);
						Socket socket = server.accept();
						ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
						OutputStream out = new FileOutputStream(filePath);) {
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
				}
			} else {
				try (ServerSocket server = new ServerSocket(dataPort);
						Socket socket = server.accept();
						ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
						OutputStream out = new FileOutputStream(filePath);) {
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
				}
			}
		}
	}

	private class ResponseThread implements Runnable {

		@Override
		public void run() {
			if (!isLast) {
				try (ServerSocket receiveResponseServer = new ServerSocket(responsePort);
						Socket receiveResponseSocket = receiveResponseServer.accept();
						ObjectInputStream receiveInput = new ObjectInputStream(receiveResponseSocket.getInputStream());
						Socket sendResponseSocket = new Socket(preIp, responsePort);
						ObjectOutputStream sendResponseOutput = new ObjectOutputStream(
								sendResponseSocket.getOutputStream());) {
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
				}
			} else {
				try (Socket respSocket = new Socket(preIp, responsePort);
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
		}
	}

	private class UncaughtPipelineException implements UncaughtExceptionHandler {
		/**
		 * 这里可以做任何针对异常的处理,比如记录日志等等
		 */
		public void uncaughtException(Thread a, Throwable e) {
			isRun = false;
			logger.error(ExceptionUtil.getStackTrace(e));
		}
	}
	
	private boolean connect(String ip, int port) throws SocketException {
		try (Socket socket = new Socket();) {
			SocketAddress address = new InetSocketAddress(ip, port);
			socket.connect(address);
		} catch (IOException e) {
			return false;
		}
		return true;
	}
}
