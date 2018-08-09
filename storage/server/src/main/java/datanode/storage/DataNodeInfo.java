package datanode.storage;

interface DataNodeInfo {
	/**
	 * 获取该存储节点唯一标识id
	 * 
	 * @return
	 */
	int getStorageId();

	String getIp();

	/**
	 * 获取该存储节点总的磁盘容量(MB)
	 * 
	 * @return
	 */
	long getDiskCapacity();

	/**
	 * 获取该存储节点已用磁盘容量(MB)
	 * 
	 * @return
	 */
	long getUsedDiskCapacity();
}
