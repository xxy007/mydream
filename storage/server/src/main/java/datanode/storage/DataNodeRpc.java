package datanode.storage;

public class DataNodeRpc implements StorageOperator{
	DataNodeManager dataNodeManager = new DataNodeManager(true);
	@Override
	public int registerDataNode(String ip, int dataPort, int rpcPort, long diskCapacity, long usedDiskCapacity) {
		return dataNodeManager.registerDataNode(ip, dataPort, rpcPort, diskCapacity, usedDiskCapacity);
	}
	@Override
	public void updateDataNode(int id, long diskCapacity, long usedDiskCapacity) {
		dataNodeManager.updateDataNode(id, diskCapacity, usedDiskCapacity);
	}
}
