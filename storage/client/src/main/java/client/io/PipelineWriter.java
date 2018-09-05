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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import datanode.storage.DataNodeStorage;
import exception.PipelineException;
import pipeline.DataPackage;
import pipeline.DataPipeline;
import pipeline.PortInfo;
import pipeline.ResponseInfo;
import rpc.RpcSender;
import tools.ExceptionUtil;

public class PipelineWriter {
	private ObjectInputStream responseReceive;
	private ObjectOutputStream dataSend;
	private String ip;
	private List<DataNodeStorage> dataNodeList;
	private Logger logger = Logger.getLogger(PipelineWriter.class);

	public PipelineWriter(List<DataNodeStorage> dataNodeList, long blockId) {
		super();
		try {
			this.dataNodeList = dataNodeList;
			setUpPipeline(blockId, dataNodeList);
		} catch (IOException e) {
			logger.error(ExceptionUtil.getStackTrace(e));
		}
		try {
			InetAddress addr = InetAddress.getLocalHost();
			ip = addr.getHostAddress().toString(); // 获取本机ip
		} catch (UnknownHostException e) {
			logger.error(ExceptionUtil.getStackTrace(e));
		}
	}

	public boolean sendPackage(DataPackage dataPackage) throws IOException {
		dataSend.writeObject(dataPackage);
		try {
			ResponseInfo responseInfo = (ResponseInfo) responseReceive.readObject();
			if (responseInfo.getResultCode() != 100) {
				throw new PipelineException(responseInfo.getReason());
			}
		} catch (ClassNotFoundException e) {
			logger.error(ExceptionUtil.getStackTrace(e));
			throw new PipelineException(ExceptionUtil.getStackTrace(e));
		}
		return true;
	}

	private void setUpPipeline(long blockId, List<DataNodeStorage> dataNodeList) throws IOException {
		int size = dataNodeList.size();
		if(size == 0) {
			throw new PipelineException("获取pipeline的dataNode列表失败");
		}
		ServerSocket receiveResponseServer = new ServerSocket(0, 1);
		int receiveResponsePort = receiveResponseServer.getLocalPort();
		receiveResponseServer.close();
		if (!setUpClientResponse(receiveResponsePort)) {
			throw new PipelineException("客户端启动接收应答失败");
		}
		Map<Integer, PortInfo> portInfoMap = new ConcurrentHashMap<>();
		for (int i = 0; i < size; i++) {
			DataNodeStorage dataNode = dataNodeList.get(i);
			DataPipeline dataPipeline = getPipelineRpc(dataNode);
			PortInfo portInfo;
			if (i == size - 1) {
				portInfo = dataPipeline.setUpPipeline(blockId, true);
			} else {
				portInfo = dataPipeline.setUpPipeline(blockId, false);
			}
			portInfoMap.put(dataNode.getStorageId(), portInfo);
		}
		for (int i = 0; i < size; i++) {
			DataNodeStorage dataNode = dataNodeList.get(i);
			DataPipeline dataPipeline = getPipelineRpc(dataNode);
			if (size == 1) {
				dataPipeline.setPipelineInfo(blockId, ip, null, 0, receiveResponsePort);
			} else if (i == 0) {
				DataNodeStorage nextDataNode = dataNodeList.get(i + 1);
				PortInfo nextPortInfo = portInfoMap.get(nextDataNode.getStorageId());
				if (nextPortInfo == null) {
					throw new PipelineException("获取portInfo信息失败");
				} else {
					dataPipeline.setPipelineInfo(blockId, ip, nextDataNode.getIp(), nextPortInfo.getReceiveDataPort(),
							receiveResponsePort);
				}
			} else if (i == (size - 1)) {
				DataNodeStorage preDataNode = dataNodeList.get(i - 1);
				PortInfo prePortInfo = portInfoMap.get(preDataNode.getStorageId());
				if (prePortInfo == null) {
					throw new PipelineException("获取portInfo信息失败");
				} else {
					dataPipeline.setPipelineInfo(blockId, preDataNode.getIp(), null, 0,
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
					dataPipeline.setPipelineInfo(blockId, preDataNode.getIp(), nextDataNode.getIp(),
							nextPortInfo.getReceiveDataPort(), prePortInfo.getReceiveResponsePort());
				}
			}
		}
		DataNodeStorage firstDataNode = dataNodeList.get(0);
		PortInfo firstPortInfo = portInfoMap.get(firstDataNode.getStorageId());
		if (!setUpClientSend(firstPortInfo.getReceiveDataPort(), firstDataNode.getIp())) {
			throw new PipelineException("客户端启动发送数据失败");
		}
	}

	@SuppressWarnings("resource")
	private boolean setUpClientResponse(int responsePort) {
		ServerSocket serverSocket = null;
		Socket socket = null;
		try {
			serverSocket = new ServerSocket(responsePort);
			socket = serverSocket.accept();
			responseReceive = new ObjectInputStream(socket.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
			try {
				closeObject(responseReceive);
				closeObject(socket);
				closeObject(serverSocket);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			return false;
		}
		return true;
	}

	@SuppressWarnings("resource")
	private boolean setUpClientSend(int dataPort, String nextIp) {
		Socket sendSocket = null;
		try {
			sendSocket = new Socket(nextIp, dataPort);
			dataSend = new ObjectOutputStream(sendSocket.getOutputStream());
		} catch (IOException e) {
			e.printStackTrace();
			try {
				if (dataSend != null) {
					dataSend.close();
				}
				if (sendSocket != null) {
					sendSocket.close();
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			return false;
		}
		return true;
	}

	private DataPipeline getPipelineRpc(DataNodeStorage dataNode) {
		RpcSender rpcSender = new RpcSender(dataNode.getIp(), dataNode.getRpcPort());
		return rpcSender.create(new DataPipeline());
	}

	public void close() throws IOException {
		if (dataSend != null) {
			dataSend.close();
		}
		if (responseReceive != null) {
			responseReceive.close();
		}
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
}
