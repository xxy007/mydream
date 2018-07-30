package datanode.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import configuration.StorageConf;

public class DataNodeManager {
	private Map<Integer, DataNodeStorage> dataNodeMap = new ConcurrentHashMap<>();
	private Map<Integer, Long> dataNodeTime = new ConcurrentHashMap<>();
	private AtomicInteger numDataNodes = new AtomicInteger(0);
	private boolean isRun = false;
	private int heartbeatSec;

	private static Logger logger = Logger.getLogger(DataNodeManager.class);

	public DataNodeManager(boolean isRun) {
		this.isRun = isRun;
		heartbeatSec = Integer.parseInt(StorageConf.getVal("datanode.heartbeat.die.second", "630"));
		Thread t = new Thread(new CheckHeartBeat());
		t.setDaemon(true);
		t.start();
	}

	public boolean isRun() {
		return isRun;
	}

	public void setRun(boolean isRun) {
		this.isRun = isRun;
	}

	public int registerDataNode(String ip, int dataPort, int rpcPort, long diskCapacity, long usedDiskCapacity) {
		int storageId = numDataNodes.incrementAndGet();
		DataNodeStorage dataNodeStorage = new DataNodeStorage(storageId, ip, dataPort, rpcPort, diskCapacity,
				usedDiskCapacity);
		dataNodeMap.put(storageId, dataNodeStorage);
		long curTime = System.currentTimeMillis();
		dataNodeTime.put(storageId, curTime);
		logger.info("registerDataNode dataNodeMap result is : " + dataNodeMap);
		logger.info("registerDataNode dataNodeTime result is : " + dataNodeTime);
		return storageId;
	}

	public void updateDataNode(int id, long diskCapacity, long usedDiskCapacity) {
		DataNodeStorage dataNodeStorage = dataNodeMap.get(id);
		if (dataNodeStorage == null) {
			logger.error("error");
			// throw error
		}
		dataNodeStorage.setDiskCapacity(diskCapacity);
		dataNodeStorage.setUsedDiskCapacity(usedDiskCapacity);
		long curTime = System.currentTimeMillis();
		dataNodeTime.put(id, curTime);
		logger.info("updateDataNode dataNodeMap result is : " + dataNodeMap);
		logger.info("updateDataNode dataNodeTime result is : " + dataNodeTime);
	}

	public DataNodeStorage getDataNode(int id) {
		return dataNodeMap.get(id);
	}
	
	class CheckHeartBeat implements Runnable {
		@Override
		public void run() {
			while(isRun) {
				long curTime = System.currentTimeMillis();
				Set<Entry<Integer, Long>> entrySet = dataNodeTime.entrySet();
				for(Entry<Integer, Long> entry : entrySet) {
					int key = entry.getKey();
					long time = entry.getValue();
					if((curTime - time) > heartbeatSec * 1000) {
						DataNodeStorage dataNode = dataNodeMap.get(key);
						logger.info("该datanode : " + dataNode.getIp() + " 已经死掉了");
						dataNodeMap.remove(key);
						dataNodeTime.remove(key);
					}
				}
			}
		}
	}
	public static void main(String[] args) {
		Map<String, String> map = new HashMap<>();
		map.put("xxy", "1");
		map.put("xxy", "2");
		logger.info(map.get("xxy"));
	}
}
