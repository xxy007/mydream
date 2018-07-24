package datanode.storage;

public class DataNodeStorage implements DataNodeInfo{
	/**
	 *datanode的唯一标识，从1开始 
	 */
	private int storageId;
	
	private String ip;
	private int dataPort;
	private int rpcPort;
	private int diskCapacity;
	private int usedDiskCapacity;
	
	public DataNodeStorage(int storageId, String ip, int dataPort, int rpcPort) {
		this.storageId = storageId;
		this.ip = ip;
		this.dataPort = dataPort;
		this.rpcPort = rpcPort;
	}
	
	public int getStorageId() {
		return storageId;
	}

	public String getIpAddr() {
		return ip;
	}

	public int getDataPort() {
		return dataPort;
	}

	public int getRpcPort() {
		return rpcPort;
	}

	public int getDiskCapacity() {
		return diskCapacity;
	}

	public int getUsedDiskCapacity() {
		return usedDiskCapacity;
	}
}