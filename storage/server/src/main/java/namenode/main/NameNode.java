package namenode.main;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import configuration.StorageConf;
import datanode.rpc.DataNodeReport;
import datanode.storage.DataNodeManager;
import exception.NameNodeInstantiationException;
import namenode.block.BlocksManager;
import namenode.namespace.FSDirectory;
import namenode.namespace.FSImage;
import namenode.namespace.INodeDirectory;
import rpc.RpcServer;

public class NameNode {
	private BlocksManager blocksManager;
	private DataNodeManager dataNodeManager;
	private FSImage fsImage;
	private RpcServer dataNodeServ;
	private RpcServer clientServ;
	private DataNodeReport dataNodeReport;
	private static Logger logger = Logger.getLogger(NameNode.class);

	public NameNode() {
		blocksManager = new BlocksManager();
		dataNodeManager = new DataNodeManager(true);
	}

	public void startDataNodeServ() {
		int nameNodeRpcPort = Integer.parseInt(StorageConf.getVal("datanode.rpc.port", "3333"));
		String nameNodeIp = StorageConf.getVal("namenode.ip", "192.168.137.130");
		dataNodeReport = new DataNodeReport(dataNodeManager, blocksManager);
		dataNodeServ = new RpcServer().setBindAddress(nameNodeIp).setPort(nameNodeRpcPort)
				.setInstance(dataNodeReport);
		logger.info("dataNodeReport is : " + dataNodeReport );
		Thread t = new Thread(dataNodeServ);
		t.setDaemon(true);
		t.start();
		logger.info("demon start namenode's dataNodeRpc Server");
	}

	public void startClientServ(INodeDirectory rootDir) {
		int clientRpcPort = Integer.parseInt(StorageConf.getVal("client.rpc.port", "1111"));
		String nameNodeIp = StorageConf.getVal("namenode.ip", "192.168.137.130");
		clientServ = new RpcServer().setBindAddress(nameNodeIp).setPort(clientRpcPort)
				.setInstance(new FSDirectory(rootDir, dataNodeManager));
		Thread t = new Thread(clientServ);
		t.setDaemon(true);
		t.start();
		logger.info("demon start namenode's clientRpc Server");
	}

	public void setup() throws InterruptedException, IOException {
		startDataNodeServ();
		long startTime = System.currentTimeMillis();
		while (!dataNodeReport.isRegisterEnd()) {
			long stopTime = System.currentTimeMillis();
			if ((stopTime - startTime) > 300000) {
				throw new NameNodeInstantiationException("namenode初始化异常，未从datanode收集到足够的注册信息");
			}
		}
		logger.info("all datanode has register end");
		startTime = System.currentTimeMillis();
		while (!dataNodeReport.isReportEnd()) {
			long stopTime = System.currentTimeMillis();
			if ((stopTime - startTime) > 300000) {
				throw new NameNodeInstantiationException("namenode初始化异常，未从datanode收集到足够的block汇报信息");
			}
		}
		
		logger.info("all datanode has report end");
		fsImage = new FSImage();
		fsImage.loadFSImage();
		INodeDirectory rootDir = fsImage.getRootDir();
		startClientServ(rootDir);
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(new PrintNodeThread(), 60, 60, TimeUnit.SECONDS);
	}

	public void join() throws InterruptedException {
		if (dataNodeServ != null)
			dataNodeServ.join();
		if (clientServ != null)
			clientServ.join();
	}

	public void stop() throws InterruptedException {
		if (dataNodeServ != null)
			dataNodeServ.stop();
		if (clientServ != null)
			clientServ.stop();
	}

	public void printNode() throws IOException {
		fsImage.printNode();
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		NameNode nameNode = new NameNode();
		nameNode.setup();
		nameNode.join();
	}

	class PrintNodeThread implements Runnable {

		@Override
		public void run() {
			try {
				printNode();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
