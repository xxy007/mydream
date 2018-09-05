package datanode.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import configuration.StorageConf;

public class DataNodeManager {
	private Map<Integer, DataNodeStorage> dataNodeMap = new ConcurrentHashMap<>();
	private Map<Integer, Long> dataNodeTime = new ConcurrentHashMap<>();
	private PriorityQueue<DataNodeStorage> dataNodeQueue = new PriorityQueue<>(new Comparator<DataNodeStorage>() {
		@Override
		public int compare(DataNodeStorage o1, DataNodeStorage o2) {
			long freeCapacity1 = o1.getDiskCapacity() - o1.getUsedDiskCapacity();
			long freeCapacity2 = o2.getDiskCapacity() - o2.getUsedDiskCapacity();
			return (freeCapacity1 < freeCapacity2) ? -1 : ((freeCapacity1 == freeCapacity2) ? 0 : 1);
		}
	});
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

	public int registerDataNode(String ip, int rpcPort, long diskCapacity, long usedDiskCapacity) {
		int storageId = numDataNodes.incrementAndGet();
		DataNodeStorage dataNodeStorage = new DataNodeStorage(storageId, rpcPort, ip, diskCapacity, usedDiskCapacity);
		dataNodeMap.put(storageId, dataNodeStorage);
		dataNodeQueue.add(dataNodeStorage);
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
			while (isRun) {
				long curTime = System.currentTimeMillis();
				Set<Entry<Integer, Long>> entrySet = dataNodeTime.entrySet();
				for (Entry<Integer, Long> entry : entrySet) {
					int key = entry.getKey();
					long time = entry.getValue();
					if ((curTime - time) > heartbeatSec * 1000) {
						DataNodeStorage dataNode = dataNodeMap.get(key);
						logger.info("该datanode : " + dataNode.getIp() + " 已经死掉了");
						dataNodeMap.remove(key);
						dataNodeTime.remove(key);
					}
				}
			}
		}
	}

	public List<DataNodeStorage> getBlockDataNode() {
		List<DataNodeStorage> dataNodeList = new ArrayList<>();
		int blockReplication = Integer.parseInt(StorageConf.getVal("block.replication"));
		if (dataNodeQueue.size() < blockReplication) {
			for (DataNodeStorage dataNode : dataNodeQueue) {
				dataNodeList.add(dataNode);
			}
		} else {
			for (int i = 0; i < blockReplication; i++) {
				DataNodeStorage dataNode = dataNodeQueue.poll();
				dataNodeList.add(dataNode);
			}
		}
		return dataNodeList;
	}

	public static void main(String[] args) {
		Map<String, String> map = new HashMap<>();
		map.put("xxy", "1");
		map.put("xxy", "2");
		logger.info(map.get("xxy"));
	}
}
