package datanode.storage;

public class DataNodeStorage implements DataNodeInfo {
	/**
	 * datanode的唯一标识，从1开始
	 */
	private int storageId;

	private String ip;
	private int dataPort;
	private int rpcPort;
	private long diskCapacity;
	private long usedDiskCapacity;

	public DataNodeStorage(int storageId, String ip, int dataPort, int rpcPort, long diskCapacity,
			long usedDiskCapacity) {
		super();
		this.storageId = storageId;
		this.ip = ip;
		this.dataPort = dataPort;
		this.rpcPort = rpcPort;
		this.diskCapacity = diskCapacity;
		this.usedDiskCapacity = usedDiskCapacity;
	}

	public int getStorageId() {
		return storageId;
	}

	public void setStorageId(int storageId) {
		this.storageId = storageId;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getDataPort() {
		return dataPort;
	}

	public void setDataPort(int dataPort) {
		this.dataPort = dataPort;
	}

	public int getRpcPort() {
		return rpcPort;
	}

	public void setRpcPort(int rpcPort) {
		this.rpcPort = rpcPort;
	}

	public long getDiskCapacity() {
		return diskCapacity;
	}

	public void setDiskCapacity(long diskCapacity) {
		this.diskCapacity = diskCapacity;
	}

	public long getUsedDiskCapacity() {
		return usedDiskCapacity;
	}

	public void setUsedDiskCapacity(long usedDiskCapacity) {
		this.usedDiskCapacity = usedDiskCapacity;
	}
}