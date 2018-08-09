package datanode.rpc;

public interface DataNodeReportOperator {
	int registerDataNode(String ip, int rpcPort, long diskCapacity, long usedDiskCapacity);
	
	void updateDataNode(int id, long diskCapacity, long usedDiskCapacity);
	
	void reportBlock(long blockId, long numBytes, int storageId);
	
	void endReportBlock(String ip);
}
