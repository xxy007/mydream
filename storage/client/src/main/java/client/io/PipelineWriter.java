package client.io;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.log4j.Logger;
import datanode.storage.DataNodeManager.PortInfo;
import datanode.storage.DataNodeStorage;
import exception.PipelineException;
import namenode.namespace.FSOperator;
import pipeline.DataPackage;
import pipeline.DataPipeline;
import pipeline.ResponseInfo;
import rpc.RpcSender;
import tools.ExceptionUtil;

public class PipelineWriter {
	private final FSOperator fsOperator;
	private ObjectInputStream responseReceive;
	private ObjectOutputStream dataSend;
	private String ip;
	private List<DataNodeStorage> dataNodeList;
	private PortInfo portInfo;
	private Logger logger = Logger.getLogger(PipelineWriter.class);

	public PipelineWriter(FSOperator fsOperator, long blockId) {
		super();
		this.fsOperator = fsOperator;
		try {
			dataNodeList = fsOperator.getDataNode();
			portInfo = fsOperator.getPortInfo();
			setUpPipeline(blockId, dataNodeList, portInfo);
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
			ResponseInfo responseInfo = (ResponseInfo)responseReceive.readObject();
			if(responseInfo.getResultCode() != 100) {
				throw new PipelineException(responseInfo.getReason());
			}
		} catch (ClassNotFoundException e) {
			logger.error(ExceptionUtil.getStackTrace(e));
			throw new PipelineException(ExceptionUtil.getStackTrace(e));
		}
		return true;
	}
	
	private void setUpPipeline(long blockId, List<DataNodeStorage> dataNodeList, PortInfo portInfo) throws IOException {
		int dataPort = portInfo.getDataPort();
		int responsePort = portInfo.getResponsePort();
		int size = dataNodeList.size();

		if (!setUpClientResponse(responsePort)) {
			throw new PipelineException("客户端启动接收应答失败");
		}
		if (size == 0) {
			DataNodeStorage dataNode = dataNodeList.get(0);
			DataPipeline dataPipeline = getPipelineRpc(dataNode);
			dataPipeline.setUpPipeline(blockId, ip, null, dataPort, responsePort, true);
		}
		for (int i = size - 1; i >= 0; i--) {
			DataNodeStorage dataNode = dataNodeList.get(i);
			DataPipeline dataPipeline = getPipelineRpc(dataNode);
			if (i == size - 1) {
				dataPipeline.setUpPipeline(blockId, dataNodeList.get(i - 1).getIp(), null, dataPort, responsePort,
						true);
			} else if (i == 0) {
				dataPipeline.setUpPipeline(blockId, ip, dataNodeList.get(i + 1).getIp(), dataPort, responsePort, false);
			} else {
				dataPipeline.setUpPipeline(blockId, dataNodeList.get(i - 1).getIp(), dataNodeList.get(i + 1).getIp(),
						dataPort, responsePort, false);
			}
		}
		if (!setUpClientSend(dataPort, dataNodeList.get(0).getIp())) {
			throw new PipelineException("客户端启动发送数据失败");
		}
	}

	@SuppressWarnings("resource")
	private boolean setUpClientResponse(int responsePort) {
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(responsePort);
			Socket socket = serverSocket.accept();
			responseReceive = new ObjectInputStream(socket.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
			try {
				if (responseReceive != null) {
					responseReceive.close();
				}
				if (serverSocket != null) {
					serverSocket.close();
				}
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
		fsOperator.recoverPortInfo(portInfo);
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
}
