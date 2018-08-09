package datanode.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import configuration.StorageConf;
import datanode.rpc.DataNodeReport;
import datanode.rpc.DataNodeReportOperator;
import rpc.RpcSender;
import tools.FileFilter;

public class DataNode {
	private static int storageId;
	private static String ip;
	private static DataNodeReportOperator dataNodeReportOperator;
	private static String dataPath;
	private static Logger logger = Logger.getLogger(DataNode.class);
	static class DiskInfo{
		long size;
		long used;
		long avail;
		public long getSize() {
			return size;
		}
		public void setSize(long size) {
			this.size = size;
		}
		public long getUsed() {
			return used;
		}
		public void setUsed(long used) {
			this.used = used;
		}
		public long getAvail() {
			return avail;
		}
		public void setAvail(long avail) {
			this.avail = avail;
		}
		@Override
		public String toString() {
			return "DiskInfo [size=" + size + ", used=" + used + ", avail=" + avail + "]";
		}
	}
	public static void main(String[] args) throws IOException {
		// 首先向namenode注册自己
		String nameNodeRpcIp = StorageConf.getVal("namenode.ip", "192.168.137.130");
		int nameNodeRpcPort = Integer.parseInt(StorageConf.getVal("datanode.rpc.port", "3333"));
		dataPath = StorageConf.getVal("dataNode.storage.dir", "/home/hadoop/xxytest/mydream/data");
		RpcSender rpcSender = new RpcSender(nameNodeRpcIp, nameNodeRpcPort);
		dataNodeReportOperator = rpcSender.create(new DataNodeReport(null));
		storageId = registerDataNode();
		logger.info("datanode " + ip + " get storageId is : " + storageId);
		reportBlock();
		int heartbeatSec = Integer.parseInt(StorageConf.getVal("datanode.heartbeat.second", "300"));
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(new HeartbeatThread(), 0, heartbeatSec, TimeUnit.SECONDS);
	}
	
	public static int registerDataNode() throws IOException {
		int rpcPort = Integer.parseInt(StorageConf.getVal("datanode.rpc.port", "3333"));
		InetAddress addr = InetAddress.getLocalHost();
		ip = addr.getHostAddress().toString(); // 获取本机ip
		DiskInfo diskInfo = getDiskInfo(dataPath);
		return dataNodeReportOperator.registerDataNode(ip, rpcPort, diskInfo.getSize(), diskInfo.getUsed());
	}
	
	public static void updateDataNode() throws IOException {
		DiskInfo diskInfo = getDiskInfo(dataPath);
		dataNodeReportOperator.updateDataNode(storageId, diskInfo.getSize(), diskInfo.getUsed());
	}
	
	public static void reportBlock() throws IOException {
		File dataFile = new File(dataPath);
		String[] files = dataFile.list(new FileFilter("blk_[0-9]+$"));
		for(String file : files) {
			long blockId = Long.parseLong(file.split("_")[1]);
			long numBytes = new File(file).length();
			dataNodeReportOperator.reportBlock(blockId, numBytes, storageId);
		}
		dataNodeReportOperator.endReportBlock(ip);
	}
	
	public static DiskInfo getDiskInfo(String dataPath) throws IOException{
		DiskInfo diskInfo = new DiskInfo();
		Runtime rt = Runtime.getRuntime();
		Process process = rt.exec(" df " + dataPath);
		try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));){
			String line;
			while ((line = in.readLine()) != null) {
				Pattern pattern = Pattern.compile("\\s+");
				Matcher m = pattern.matcher(line);
				String context = m.replaceAll(" ");
				String[] info = context.split(" ");
				if("Filesystem".equals(info[0])) {
					continue;
				}
				logger.info(Arrays.asList(info));
				long size = Long.parseLong(info[1]);
				long used = Long.parseLong(info[2]);
				long avail = Long.parseLong(info[3]);
				diskInfo.setSize(size);
				diskInfo.setUsed(used);
				diskInfo.setAvail(avail);
				logger.info(size + " " + used + " " + avail);
			}
		}
		return diskInfo;
	}
	
	static class HeartbeatThread implements Runnable{

		@Override
		public void run() {
			try {
				updateDataNode();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
