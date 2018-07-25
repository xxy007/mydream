package datanode.storage;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import configuration.StorageConf;

public class DataNodeManager {
	private Map<Integer, DataNodeStorage> datanodeMap = new ConcurrentHashMap<>();
	private Map<Integer, Long> datanodeTime = new ConcurrentHashMap<>();
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
		datanodeMap.put(storageId, dataNodeStorage);
		long curTime = System.currentTimeMillis();
		datanodeTime.put(storageId, curTime);
		return storageId;
	}

	public void updateDataNode(int id, long diskCapacity, long usedDiskCapacity) {
		DataNodeStorage dataNodeStorage = datanodeMap.get(id);
		if (dataNodeStorage == null) {
			logger.error("error");
			// throw error
		}
		dataNodeStorage.setDiskCapacity(diskCapacity);
		dataNodeStorage.setUsedDiskCapacity(usedDiskCapacity);
		long curTime = System.currentTimeMillis();
		datanodeTime.put(id, curTime);
	}

	class CheckHeartBeat implements Runnable {
		@Override
		public void run() {
			while(isRun) {
				long curTime = System.currentTimeMillis();
				Set<Entry<Integer, Long>> entrySet = datanodeTime.entrySet();
				for(Entry<Integer, Long> entry : entrySet) {
					int key = entry.getKey();
					long time = entry.getValue();
					if((curTime - time) > heartbeatSec) {
						logger.info("该datanode已经死掉了");
						datanodeMap.remove(key);
						datanodeTime.remove(key);
					}
				}
			}
		}
	}
}
