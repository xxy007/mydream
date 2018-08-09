package namenode.main;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import configuration.StorageConf;
import datanode.rpc.DataNodeReport;
import datanode.storage.DataNodeManager;
import datanode.storage.DataNodeStorage;
import exception.NameNodeInstantiationException;
import namenode.block.BlockInfo;
import namenode.block.BlocksManager;
import namenode.namespace.FSDirectory;
import namenode.namespace.FSImage;
import namenode.namespace.INodeDirectory;
import rpc.RpcServer;

public class NameNode {
	private BlocksManager blocksManager;
	private DataNodeManager dataNodeManager;
	private FSDirectory fsDirectory;
	private FSImage fsImage;
	private Set<String> ipRegisterSet;
	private Set<String> ipReportSet;
	private RpcServer dataNodeServ;
	private RpcServer clientServ;
	private static Logger logger = Logger.getLogger(NameNode.class);
	public NameNode() {
		blocksManager = new BlocksManager();
		dataNodeManager = new DataNodeManager(true);
		ipRegisterSet = new ConcurrentSkipListSet<>();
		ipReportSet = new ConcurrentSkipListSet<>();
	}

	public void startDataNodeServ() {
		int nameNodeRpcPort = Integer.parseInt(StorageConf.getVal("datanode.rpc.port", "3333"));
		String nameNodeIp = StorageConf.getVal("namenode.ip", "192.168.137.130");
		dataNodeServ = new RpcServer().setBindAddress(nameNodeIp).setPort(nameNodeRpcPort).setInstance(new DataNodeReport(this));
		Thread t = new Thread(dataNodeServ);
		t.setDaemon(true);
		t.start();
		logger.info("demon start namenode's dataNodeRpc Server");
	}

	public void startClientServ(INodeDirectory rootDir) {
		int clientRpcPort = Integer.parseInt(StorageConf.getVal("client.rpc.port", "1111"));
		String nameNodeIp = StorageConf.getVal("namenode.ip", "192.168.137.130");
		clientServ = new RpcServer().setBindAddress(nameNodeIp).setPort(clientRpcPort).setInstance(new FSDirectory(rootDir));
		Thread t = new Thread(clientServ);
		t.setDaemon(true);
		t.start();
		logger.info("demon start namenode's clientRpc Server");
	}
	
	public void setup() throws InterruptedException, IOException {
		startDataNodeServ();
		getDataNodeIp();
		logger.info("ins ipReportSet is : " + ipReportSet);
		logger.info("ins ipRegisterSet is : " + ipRegisterSet);
		long startTime = System.currentTimeMillis();
		while (!ipRegisterSet.isEmpty()) {
//			logger.info("ipRegisterSet is : " + ipRegisterSet);
			long stopTime = System.currentTimeMillis();
			if ((stopTime - startTime) > 300000) {
				throw new NameNodeInstantiationException("namenode初始化异常，未从datanode收集到足够的注册信息");
			}
		}
		logger.info("all datanode has register end");
		startTime = System.currentTimeMillis();
		while (!ipReportSet.isEmpty()) {
//			logger.info("ipReportSet is : " + ipReportSet);
			long stopTime = System.currentTimeMillis();
			if ((stopTime - startTime) > 300000) {
				throw new NameNodeInstantiationException("namenode初始化异常，未从datanode收集到足够的block汇报信息");
			}
		}
		logger.info("all datanode has report end");
		fsImage = new FSImage();
		fsImage.loadFSImage();
		INodeDirectory rootDir = fsImage.getRootDir();
		startClientServ(rootDir);
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(new PrintNodeThread(), 60, 60, TimeUnit.SECONDS);
	}

	public void join() throws InterruptedException {
		if(dataNodeServ != null)
			dataNodeServ.join();
		if(clientServ != null)
			clientServ.join();
	}
	
	public void stop() throws InterruptedException {
		if(dataNodeServ != null)
			dataNodeServ.stop();
		if(clientServ != null)
			clientServ.stop();
	}

	public void printNode() throws IOException {
		fsImage.printNode();
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		NameNode nameNode = new NameNode();
		nameNode.setup();
		nameNode.join();
	}

	public int registerDataNode(String ip, int rpcPort, long diskCapacity, long usedDiskCapacity) {
		ipRegisterSet.remove(ip);
		logger.info("datanode : " + ip + " has register end");
		logger.info("ipRegisterSet is : " + ipRegisterSet);
		return dataNodeManager.registerDataNode(ip, rpcPort, diskCapacity, usedDiskCapacity);
	}

	public void updateDataNode(int storageId, long diskCapacity, long usedDiskCapacity) {
		dataNodeManager.updateDataNode(storageId, diskCapacity, usedDiskCapacity);
	}

	public void reportBlock(long blockId, long numBytes, int storageId) {
		DataNodeStorage dataNodeStorage = dataNodeManager.getDataNode(storageId);
		BlockInfo blockInfo = new BlockInfo(blockId, numBytes);
		BlockInfo srcBlockInfo = blocksManager.getBlockInfo(blockInfo);
		if (srcBlockInfo == null) {
			blockInfo.addDataNodeStorage(dataNodeStorage);
			blocksManager.putBlockInfo(blockInfo);
		} else {
			srcBlockInfo.addDataNodeStorage(dataNodeStorage);
		}
		logger.info("datanode : " + dataNodeStorage.getIp() + "has report end");
	}

	public void endReportBlock(String ip) {
		ipReportSet.remove(ip);
		logger.info("ipReportSet is : " + ipReportSet);
		logger.info("datanode : " + ip + "has report end");
	}

	public void setDataNode(long blockId, long numBytes, int storageId) {
		DataNodeStorage dataNodeStorage = dataNodeManager.getDataNode(storageId);
		BlockInfo blockInfo = new BlockInfo(blockId, numBytes);
		BlockInfo srcBlockInfo = blocksManager.getBlockInfo(blockInfo);
		if (srcBlockInfo == null) {
			blockInfo.addDataNodeStorage(dataNodeStorage);
			blocksManager.putBlockInfo(blockInfo);
		} else {
			srcBlockInfo.addDataNodeStorage(dataNodeStorage);
		}
	}

	public void getDataNodeIp() {
		String dataNodeIps = StorageConf.getVal("datanode.ip", "127.0.0.1");
		for (String dataNodeIp : dataNodeIps.split(",")) {
			ipReportSet.add(dataNodeIp);
			ipRegisterSet.add(dataNodeIp);
		}
	}
	class PrintNodeThread implements Runnable{

		@Override
		public void run() {
			try {
				printNode();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
