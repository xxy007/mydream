package client.main;

import java.io.IOException;

import org.apache.log4j.Logger;

import configuration.StorageConf;
import datanode.rpc.DataNodeReportRpc;
import namenode.namespace.FSDirectory;
import namenode.namespace.FSOperator;

public class Client {
	private static Logger logger = Logger.getLogger(Client.class);
	public static void main(String[] args) throws IOException {
		String nameNodeRpcIp = StorageConf.getVal("namenode.ip", "192.168.137.130");
		int nameNodeRpcPort = Integer.parseInt(StorageConf.getVal("client.rpc.port", "1111"));
		DataNodeReportRpc dataNodeReportRpc = new DataNodeReportRpc(nameNodeRpcIp, nameNodeRpcPort);
		FSOperator fsOperator = dataNodeReportRpc.create(new FSDirectory(null));
		boolean result = false;
		result = fsOperator.createDir("/mydream");
		logger.info("create dir /mydream result is : " + result);
		result = fsOperator.createDir("/mydream/name");
		logger.info("create dir /mydream/name result is : " + result);
		result = fsOperator.createDir("/mydream/name/xxy");
		logger.info("create dir /mydream/name/xxy result is : " + result);
		result = fsOperator.createDir("/mydream/name/zmz");
		logger.info("create dir /mydream/name/zmz result is : " + result);
		result = fsOperator.createDir("/mydream/name/xzf");
		logger.info("create dir /mydream/name/xzf result is : " + result);
		result = fsOperator.createDir("/mydream/name/jb");
		logger.info("create dir /mydream/name/jb result is : " + result);
	}
}
