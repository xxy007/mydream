package datanode.rpc;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import configuration.StorageConf;
import datanode.storage.DataNodeManager;
import datanode.storage.DataNodeStorage;
import namenode.block.BlockInfo;
import namenode.block.BlocksManager;

public class DataNodeReport implements DataNodeReportOperator{
	private DataNodeManager dataNodeManager;
	private BlocksManager blocksManager;
	private Set<String> ipRegisterSet;
	private Set<String> ipReportSet;
	private volatile boolean isRegisterEnd = false;
	private volatile boolean isReportEnd = false;
	private static Logger logger = Logger.getLogger(DataNodeReport.class);
	
	public DataNodeReport(DataNodeManager dataNodeManager, BlocksManager blocksManager) {
		this.dataNodeManager = dataNodeManager;
		this.blocksManager = blocksManager;
		ipReportSet = new HashSet<>();
		ipRegisterSet = new HashSet<>();
		getDataNodeIp();
	}
	
	@Override
	public synchronized int registerDataNode(String ip, int rpcPort, long diskCapacity, long usedDiskCapacity) {
		ipRegisterSet.remove(ip);
		logger.info("datanode : " + ip + " has register end");
		logger.info("ipRegisterSet is : " + ipRegisterSet);
		if(ipRegisterSet.isEmpty()) {
			isRegisterEnd = true;
		}
		return dataNodeManager.registerDataNode(ip, rpcPort, diskCapacity, usedDiskCapacity);
	}
	@Override
	public synchronized void updateDataNode(int storageId, long diskCapacity, long usedDiskCapacity) {
		dataNodeManager.updateDataNode(storageId, diskCapacity, usedDiskCapacity);
	}
	@Override
	public synchronized void reportBlock(long blockId, long numBytes, int storageId) {
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

	@Override
	public synchronized void endReportBlock(String ip) {
		ipReportSet.remove(ip);
		logger.info("datanode : " + ip + "has report end");
		logger.info("ipReportSet is : " + ipReportSet);
		if(ipReportSet.isEmpty())
		{
			isReportEnd = true;
		}
	}
	private void getDataNodeIp() {
		String dataNodeIps = StorageConf.getVal("datanode.ip", "127.0.0.1");
		for (String dataNodeIp : dataNodeIps.split(",")) {
			ipReportSet.add(dataNodeIp);
			ipRegisterSet.add(dataNodeIp);
		}
	}

	public boolean isRegisterEnd() {
		return isRegisterEnd;
	}

	public boolean isReportEnd() {
		return isReportEnd;
	}
}
