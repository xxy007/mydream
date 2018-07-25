package datanode.storage;

public interface StorageOperator {
	int registerDataNode( String ip, int dataPort, int rpcPort, long diskCapacity, long usedDiskCapacity);
	
	void updateDataNode(int id, long diskCapacity, long usedDiskCapacity);
}
