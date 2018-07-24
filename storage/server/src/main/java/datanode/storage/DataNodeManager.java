package datanode.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import configuration.StorageConf;

public class DataNodeManager {
	  private final Map<Integer, DataNodeStorage> datanodeMap = new ConcurrentHashMap<>();

	  private int numDataNodes;
	  
	  public DataNodeManager() {
		  int dataPort = Integer.parseInt(StorageConf.getVal("data.port"));
		  int rpcPort = Integer.parseInt(StorageConf.getVal("rpc.port"));
		  String dataNodeInfo = StorageConf.getVal("datanode.ip");
		  int dataNodeCount = 0;
		  for(String ip : dataNodeInfo.split(";"))
		  {
			  ++dataNodeCount;
			  DataNodeStorage dataNodeStorage = new DataNodeStorage(dataNodeCount, ip, dataPort, rpcPort);
			  datanodeMap.put(dataNodeCount, dataNodeStorage);
		  }
		  numDataNodes = dataNodeCount;
	  }
}
