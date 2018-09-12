package client.io;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import datanode.storage.DataNodeStorage;
import exception.PipelineException;
import pipeline.DataPackage;
import pipeline.DataPipeline;
import pipeline.DataPipelineOperator;
import pipeline.PortInfo;
import pipeline.ResponseInfo;
import rpc.RpcSender;
import tools.ExceptionUtil;

public class PipelineWriter {
	private String ip;
	private List<DataNodeStorage> dataNodeList;
	private BlockingQueue<ResponseInfo> responseInfoBlockQueue;
	private BlockingQueue<DataPackage> dataBlockQueue;
	private boolean isRun;
	private Logger logger = Logger.getLogger(PipelineWriter.class);

	public PipelineWriter(List<DataNodeStorage> dataNodeList, long blockId) {
		super();
		try {
			InetAddress addr = InetAddress.getLocalHost();
			ip = addr.getHostAddress().toString(); // 获取本机ip
			logger.info("client ip is : " + ip);
		} catch (UnknownHostException e) {
			logger.error(ExceptionUtil.getStackTrace(e));
		}
		try {
			this.dataNodeList = dataNodeList;
			setUpPipeline(blockId, dataNodeList);
		} catch (IOException e) {
			logger.error(ExceptionUtil.getStackTrace(e));
		}
		isRun = true;
		responseInfoBlockQueue = new LinkedBlockingQueue<>();
		dataBlockQueue = new LinkedBlockingQueue<>();
	}

	public boolean sendPackage(DataPackage dataPackage) throws IOException {
		dataBlockQueue.add(dataPackage);
		try {
			ResponseInfo responseInfo = responseInfoBlockQueue.poll(600, TimeUnit.SECONDS);
			if ((responseInfo == null) || responseInfo.getResultCode() != 100) {
				throw new PipelineException(responseInfo.getReason());
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	private void setUpPipeline(long blockId, List<DataNodeStorage> dataNodeList) throws IOException {
		int size = dataNodeList.size();
		if (size == 0) {
			throw new PipelineException("获取pipeline的dataNode列表失败");
		}
		ServerSocket receiveResponseServer = new ServerSocket(0, 1);
		int receiveResponsePort = receiveResponseServer.getLocalPort();
		receiveResponseServer.close();
		logger.info("client receive response port is : " + receiveResponsePort);
		ResponseRunnable ResponseRunnable = new ResponseRunnable(receiveResponsePort);
		// Thread ResponseThread = new Thread(ResponseRunnable);
		// ResponseThread.setUncaughtExceptionHandler(new UncaughtPipelineException());
		// ResponseThread.run();
		logger.info("client begin setup receive response,receiveResponsePort is : " + receiveResponsePort);
		Executors.newFixedThreadPool(1).execute(ResponseRunnable);
		logger.info("client setup receive response sucess,receiveResponsePort is : " + receiveResponsePort);
		Map<Integer, PortInfo> portInfoMap = new ConcurrentHashMap<>();
		for (int i = 0; i < size; i++) {
			DataNodeStorage dataNode = dataNodeList.get(i);
			DataPipelineOperator dataPipelineOper = getPipelineRpc(dataNode);
			if (dataPipelineOper == null) {
				logger.error("client get " + dataNode.getIp() + " : " + dataNode.getRpcPort() + " rpc server fault");
				return;
			}
			PortInfo portInfo;
			if (i == size - 1) {
				portInfo = dataPipelineOper.setUpPipeline(blockId, true);
			} else {
				portInfo = dataPipelineOper.setUpPipeline(blockId, false);
			}
			if (portInfo == null) {
				logger.error("client get " + dataNode.getIp() + " : " + dataNode.getRpcPort() + " rpc server fault");
				return;
			}
			logger.info("setup " + dataNode.getIp() + " : " + dataNode.getRpcPort() + " rpc server sucess");
			portInfoMap.put(dataNode.getStorageId(), portInfo);
		}
		logger.info("portInfoMap is : " + portInfoMap);
		for (int i = 0; i < size; i++) {
			DataNodeStorage dataNode = dataNodeList.get(i);
			DataPipelineOperator dataPipelineOper = getPipelineRpc(dataNode);
			if (size == 1) {
				dataPipelineOper.setPipelineInfo(blockId, ip, null, 0, receiveResponsePort);
			} else if (i == 0) {
				DataNodeStorage nextDataNode = dataNodeList.get(i + 1);
				PortInfo nextPortInfo = portInfoMap.get(nextDataNode.getStorageId());
				if (nextPortInfo == null) {
					throw new PipelineException("获取portInfo信息失败");
				} else {
					dataPipelineOper.setPipelineInfo(blockId, ip, nextDataNode.getIp(),
							nextPortInfo.getReceiveDataPort(), receiveResponsePort);
				}
			} else if (i == (size - 1)) {
				DataNodeStorage preDataNode = dataNodeList.get(i - 1);
				PortInfo prePortInfo = portInfoMap.get(preDataNode.getStorageId());
				if (prePortInfo == null) {
					throw new PipelineException("获取portInfo信息失败");
				} else {
					dataPipelineOper.setPipelineInfo(blockId, preDataNode.getIp(), null, 0,
							prePortInfo.getReceiveResponsePort());
				}
			} else {
				DataNodeStorage nextDataNode = dataNodeList.get(i + 1);
				DataNodeStorage preDataNode = dataNodeList.get(i - 1);
				PortInfo nextPortInfo = portInfoMap.get(nextDataNode.getStorageId());
				PortInfo prePortInfo = portInfoMap.get(preDataNode.getStorageId());
				if (nextPortInfo == null || prePortInfo == null) {
					throw new PipelineException("获取portInfo信息失败");
				} else {
					dataPipelineOper.setPipelineInfo(blockId, preDataNode.getIp(), nextDataNode.getIp(),
							nextPortInfo.getReceiveDataPort(), prePortInfo.getReceiveResponsePort());
				}
			}
		}
		DataNodeStorage firstDataNode = dataNodeList.get(0);
		PortInfo firstPortInfo = portInfoMap.get(firstDataNode.getStorageId());
		DataRunnable dataRunnable = new DataRunnable(firstDataNode.getIp(), firstPortInfo.getReceiveDataPort());
		// Thread ResponseThread = new Thread(ResponseRunnable);
		// ResponseThread.setUncaughtExceptionHandler(new UncaughtPipelineException());
		// ResponseThread.run();
		logger.info("client begin setup data send,send ip is : " + firstDataNode.getIp() + " send Port is : "
				+ firstPortInfo.getReceiveDataPort());
		Executors.newFixedThreadPool(1).execute(dataRunnable);
		logger.info("client start send data sucess");
	}

	private DataPipelineOperator getPipelineRpc(DataNodeStorage dataNode) {
		RpcSender rpcSender = new RpcSender(dataNode.getIp(), dataNode.getRpcPort());
		logger.info("client build datanode,ip is : " + dataNode.getIp() + " and port is : " + dataNode.getRpcPort());
		return rpcSender.create(new DataPipeline());
	}

	public void close() throws IOException {
		isRun = false;
	}

	public List<DataNodeStorage> getDataNodeList() {
		return dataNodeList;
	}

	private void closeObject(Object obj) throws IOException {
		if (obj != null && obj instanceof java.io.Closeable) {
			java.io.Closeable closeObj = (java.io.Closeable) obj;
			closeObj.close();
		}
	}

	private class ResponseRunnable implements Runnable {
		private int receiveResponsePort;

		public ResponseRunnable(int receiveResponsePort) {
			super();
			this.receiveResponsePort = receiveResponsePort;
		}

		@Override
		public void run() {
			ServerSocket serverSocket = null;
			Socket socket = null;
			ObjectInputStream responseReceive = null;
			try {
				serverSocket = new ServerSocket(receiveResponsePort);
				socket = serverSocket.accept();
				serverSocket.setSoTimeout(60000);
				responseReceive = new ObjectInputStream(socket.getInputStream());
				while (isRun) {
					ResponseInfo responseInfo = (ResponseInfo) responseReceive.readObject();
					responseInfoBlockQueue.add(responseInfo);
				}
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
				try {
					closeObject(responseReceive);
					closeObject(socket);
					closeObject(serverSocket);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

	private class DataRunnable implements Runnable {
		private String nextIp;
		private int nextPort;

		public DataRunnable(String nextIp, int nextPort) {
			super();
			this.nextIp = nextIp;
			this.nextPort = nextPort;
		}

		@Override
		public void run() {
			try (Socket sendSocket = new Socket(nextIp, nextPort);
					ObjectOutputStream dataSend = new ObjectOutputStream(sendSocket.getOutputStream());) {
				while (isRun) {
					DataPackage dataPackage = dataBlockQueue.take();
					dataSend.writeObject(dataPackage);
				}
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
