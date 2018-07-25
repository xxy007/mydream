package datanode.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import configuration.StorageConf;
import datanode.rpc.DataNodeReportRpc;
import datanode.storage.DataNodeRpc;

public class SetupDataNode {
	private static int storageId;
	private static DataNodeRpc dataNodeRpc;
	private static String dataPath;
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
		String nameNodeRpcIp = StorageConf.getVal("namenode.rpc.ip");
		int nameNodeRpcPort = Integer.parseInt(StorageConf.getVal("namenode.rpc.port"));
		dataPath = StorageConf.getVal("dataNode.storage.dir");
		DataNodeReportRpc dataNodeReportRpc = new DataNodeReportRpc(nameNodeRpcIp, nameNodeRpcPort);
		dataNodeRpc = dataNodeReportRpc.create(DataNodeRpc.class);
		storageId = registerDataNode();
		int heartbeatSec = Integer.parseInt(StorageConf.getVal("datanode.heartbeat.second", "300"));
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(new HeartbeatThread(), 0, heartbeatSec, TimeUnit.SECONDS);
	}
	
	public static int registerDataNode() throws IOException {
		int dataPort = Integer.parseInt(StorageConf.getVal("data.port"));
		int rpcPort = Integer.parseInt(StorageConf.getVal("rpc.port"));
		InetAddress addr = InetAddress.getLocalHost();
		String ip = addr.getHostAddress().toString(); // 获取本机ip
		DiskInfo diskInfo = getDiskInfo(dataPath);
		return dataNodeRpc.registerDataNode(ip, dataPort, rpcPort, diskInfo.getSize(), diskInfo.getUsed());
	}
	
	public static void updateDataNode() throws IOException {
		DiskInfo diskInfo = getDiskInfo(dataPath);
		dataNodeRpc.updateDataNode(storageId, diskInfo.getSize(), diskInfo.getUsed());
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
				System.out.println(Arrays.asList(info));
				long size = Long.parseLong(info[1]);
				long used = Long.parseLong(info[2]);
				long avail = Long.parseLong(info[3]);
				diskInfo.setSize(size);
				diskInfo.setUsed(used);
				diskInfo.setAvail(avail);
				System.out.println(size + " " + used + " " + avail);
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
