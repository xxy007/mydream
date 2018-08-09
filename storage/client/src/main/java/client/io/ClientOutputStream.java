package client.io;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.log4j.Logger;

import configuration.StorageConf;
import namenode.block.BlockInfo;
import namenode.namespace.FSDirectory;
import namenode.namespace.FSOperator;
import pipeline.DataPackage;
import rpc.RpcSender;
import tools.ExceptionUtil;
import tools.Sequence;

public class ClientOutputStream extends MyDreamOutputStream {
	// 一个chunk 最多512字节
	// 一个package最多包括128个chunk,假设一个package包括n个完整chunk和一个不完整chunk(大小为m)，
	// 这样一个package有512*n + m + 4*128(最大头)
	// 一个block为128M，最多包括2048个package的真正数据
	private DataPackage dataPackage;
	private long blockId;
	private final String filePath;
	private final FSOperator fsOperator;
	private PipelineWriter pipelineWriter; 
	private Logger logger = Logger.getLogger(ClientOutputStream.class);

	public ClientOutputStream(int chunkByteNums, int checksumByteNums, String filePath) {
		super(chunkByteNums);
		this.filePath = filePath;
		this.fsOperator = getFsOperator();
		blockId = Sequence.nextVal();
		long packageReqId = Sequence.nextVal();
		dataPackage = new DataPackage(packageReqId, 0, blockId);
		try {
			InetAddress addr = InetAddress.getLocalHost();
			addr.getHostAddress().toString();
		} catch (UnknownHostException e) {
			logger.error(ExceptionUtil.getStackTrace(e));
		}
		pipelineWriter = new PipelineWriter(fsOperator, blockId);
	}

	@Override
	protected void writeChunk(byte[] b, int bOffset, int bLen, byte[] checksum, int checksumOffset, int checksumLen)
			throws IOException {
		dataPackage.writeHead(checksum, checksumOffset, checksumLen);
		dataPackage.writeData(b, bOffset, bLen);
		dataPackage.incrChunkNums();
		if (dataPackage.getMaxChunks() == dataPackage.getCurChunkNums()) {
			pipelineWriter.sendPackage(dataPackage);
			int offsetInBlock = dataPackage.getOffsetInBlock();
			if (offsetInBlock == 2048) {
				BlockInfo blockInfo = new BlockInfo(blockId, 128 * 1024 * 1024);
				blockInfo.setDataNodeList(pipelineWriter.getDataNodeList());
				fsOperator.addBlockInfo(filePath, blockInfo);
				dataPackage = new DataPackage(Sequence.nextVal(), offsetInBlock, blockId);
				pipelineWriter.sendPackage(dataPackage);
				pipelineWriter.close();
				pipelineWriter = new PipelineWriter(fsOperator, Sequence.nextVal());
			} else {
				dataPackage = new DataPackage(Sequence.nextVal(), offsetInBlock++, blockId);
			}
		}
	}
	
	private FSOperator getFsOperator() {
		String nameNodeRpcIp = StorageConf.getVal("namenode.ip", "192.168.137.130");
		int nameNodeRpcPort = Integer.parseInt(StorageConf.getVal("client.rpc.port", "1111"));
		RpcSender rpcSender = new RpcSender(nameNodeRpcIp, nameNodeRpcPort);
		return rpcSender.create(new FSDirectory(null));
	}
	
	@Override
	public void close() throws IOException {
		super.close();
		pipelineWriter.sendPackage(dataPackage);
		dataPackage = new DataPackage(Sequence.nextVal(), dataPackage.getOffsetInBlock(), blockId);
		pipelineWriter.sendPackage(dataPackage);
		pipelineWriter.close();
	}
}
