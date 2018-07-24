package datanode.storage;

interface DataNodeInfo {
	/**
	 * 获取该存储节点唯一标识id
	 * 
	 * @return
	 */
	int getStorageId();

	String getIpAddr();

	/**
	 * 获取该存储节点数据传输端口
	 * 
	 * @return
	 */
	int getDataPort();

	/**
	 * 获取该存储节点RPC调用端口
	 * 
	 * @return
	 */
	int getRpcPort();

	/**
	 * 获取该存储节点总的磁盘容量(MB)
	 * 
	 * @return
	 */
	int getDiskCapacity();

	/**
	 * 获取该存储节点已用磁盘容量(MB)
	 * 
	 * @return
	 */
	int getUsedDiskCapacity();
}
