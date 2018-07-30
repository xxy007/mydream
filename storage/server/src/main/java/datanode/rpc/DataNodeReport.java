package datanode.rpc;

import namenode.main.NameNode;

public class DataNodeReport implements DataNodeReportOperator{
	private final NameNode nameNode;
	
	public DataNodeReport(NameNode nameNode) {
		this.nameNode = nameNode;
	}
	
	@Override
	public int registerDataNode(String ip, int dataPort, int rpcPort, long diskCapacity, long usedDiskCapacity) {
		return nameNode.registerDataNode(ip, dataPort, rpcPort, diskCapacity, usedDiskCapacity);
	}
	@Override
	public void updateDataNode(int storageId, long diskCapacity, long usedDiskCapacity) {
		nameNode.updateDataNode(storageId, diskCapacity, usedDiskCapacity);
	}
	@Override
	public void reportBlock(long blockId, long numBytes, int storageId) {
		nameNode.reportBlock(blockId, numBytes, storageId);
	}

	@Override
	public void endReportBlock(String ip) {
		nameNode.endReportBlock(ip);
	}
}
