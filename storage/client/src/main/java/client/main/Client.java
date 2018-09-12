package client.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import client.io.ClientOutputStream;
import client.rpc.ClientRpc;
import configuration.StorageConf;
import namenode.namespace.FSDirectory;
import namenode.namespace.FSOperator;

public class Client {
	private static Logger logger = Logger.getLogger(Client.class);
	public static void main(String[] args) throws IOException {
		String nameNodeRpcIp = StorageConf.getVal("namenode.ip", "192.168.137.130");
		int nameNodeRpcPort = Integer.parseInt(StorageConf.getVal("client.rpc.port", "1111"));
		ClientRpc clientSender = new ClientRpc(nameNodeRpcIp, nameNodeRpcPort);
		FSOperator fsOperator = clientSender.create(new FSDirectory(null, null));
		boolean result = false;
		result = fsOperator.createDir("/mydream");
		logger.info("create dir /mydream result is : " + result);
		ClientOutputStream out = new ClientOutputStream(fsOperator, 512, 4, "/mydream/test.txt");
		File file = new File("/home/hadoop/xxytest/myDreamTest/test.txt");
		FileInputStream input = new FileInputStream(file);
		byte[] readByte = new byte[100];
		int readNum = 0;
		while((readNum = input.read(readByte)) != -1) {
			out.write(readByte, 0, readNum);
		}
		input.close();
		out.flush();
		out.close();
		
//		result = fsOperator.createDir("/mydream/name");
//		logger.info("create dir /mydream/name result is : " + result);
//		result = fsOperator.createDir("/mydream/name/xxy");
//		logger.info("create dir /mydream/name/xxy result is : " + result);
//		result = fsOperator.createDir("/mydream/name/zmz");
//		logger.info("create dir /mydream/name/zmz result is : " + result);
//		result = fsOperator.createDir("/mydream/name/xzf");
//		logger.info("create dir /mydream/name/xzf result is : " + result);
//		result = fsOperator.createDir("/mydream/name/jb");
//		logger.info("create dir /mydream/name/jb result is : " + result);
	}
}
