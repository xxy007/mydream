package client.io;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.log4j.Logger;
import datanode.storage.DataNodeStorage;
import namenode.block.BlockInfo;
import namenode.namespace.FSOperator;
import pipeline.DataPackage;
import tools.ByteObject;
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

	public ClientOutputStream(FSOperator fsOperator, int chunkByteNums, int checksumByteNums, String filePath) {
		super(chunkByteNums);
		this.filePath = filePath;
		this.fsOperator = fsOperator;
		blockId = Sequence.nextVal();
		long packageReqId = Sequence.nextVal();
		dataPackage = new DataPackage(packageReqId, 0, blockId);
		try {
			InetAddress addr = InetAddress.getLocalHost();
			addr.getHostAddress().toString();
		} catch (UnknownHostException e) {
			logger.error(ExceptionUtil.getStackTrace(e));
		}
		List<DataNodeStorage> dataNodeList = fsOperator.getDataNode();
		logger.info("client get datanode list is : " + dataNodeList);
		pipelineWriter = new PipelineWriter(dataNodeList, blockId);
	}

	@Override
	protected void writeChunk(byte[] b, int bOffset, int bLen, byte[] checksum, int checksumOffset, int checksumLen)
			throws IOException {
		logger.info("client is writting one Chunk");
		logger.info("write chunk context is : " + new String(b) + " write chunk check context is : " + new String(checksum));
		dataPackage.writeHead(checksum, checksumOffset, checksumLen);
		dataPackage.writeData(b, bOffset, bLen);
		logger.info("this write checksum is : " + ByteObject.byteArrayToInt(checksum) + "checksumLen is : " + checksumLen + "checksumOffset is : " + checksumOffset);
		logger.info("this write data is : " + new String(b) + "bLen is : " + bLen + "bOffset is : " + bOffset);
		logger.info("this get checksum is : " + ByteObject.byteArrayToInt(dataPackage.getPackageBuf(), 0, 4) + "checksumLen is : " + dataPackage.getChunksumEnd() + "checksumOffset is : " + 0);
		logger.info("this get data is : " + new String(dataPackage.getPackageBuf()) + "bLen is : " + dataPackage.getDataEnd() + "bOffset is : " + dataPackage.getDataStart());
		dataPackage.incrChunkNums();
		if (dataPackage.getMaxChunks() == dataPackage.getCurChunkNums()) {
			logger.info("dataPackage's CurChunkNums is : " + dataPackage.getCurChunkNums());
			logger.info("dataPackage's MaxChunks is : " + dataPackage.getMaxChunks());
			pipelineWriter.sendPackage(dataPackage);
			int offsetInBlock = dataPackage.getOffsetInBlock();
			if (offsetInBlock == 2048) {
				BlockInfo blockInfo = new BlockInfo(blockId, 128 * 1024 * 1024);
				blockInfo.setDataNodeList(pipelineWriter.getDataNodeList());
				fsOperator.addBlockInfo(filePath, blockInfo);
				dataPackage = new DataPackage(Sequence.nextVal(), offsetInBlock, blockId);
				pipelineWriter.sendPackage(dataPackage);
				pipelineWriter.close();
				pipelineWriter = new PipelineWriter(fsOperator.getDataNode(), Sequence.nextVal());
			} else {
				dataPackage = new DataPackage(Sequence.nextVal(), offsetInBlock++, blockId);
			}
		}
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
