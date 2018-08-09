package pipeline;

import java.io.Serializable;

public class DataPackage implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6455526921412407929L;
	private long reqId;
	private final int maxChunks = 128;
	private final int packageByteNums = 66048;
	private int chunkNums = 0;
	private byte[] packageBuf;
	private final int offsetInBlock; // offset in block
	private int checksumEnd;
	private int dataStart;
	private int dataEnd;
	private final long blockId;
	private static final int MAX_HEAD = 4 * 128;

	public DataPackage(long reqId, int offsetInBlock, long blockId) {
		packageBuf = new byte[packageByteNums];
		this.offsetInBlock = offsetInBlock;
		this.blockId = blockId;
		this.dataStart = this.dataEnd = MAX_HEAD;
		this.reqId = reqId;
	}

	public long getReqId() {
		return reqId;
	}

	public int getChunksumEnd() {
		return checksumEnd;
	}

	public int getDataStart() {
		return dataStart;
	}

	public int getDataEnd() {
		return dataEnd;
	}

	public int getCurChunkNums() {
		return chunkNums;
	}

	public int getMaxChunks() {
		return maxChunks;
	}

	public int incrChunkNums() {
		return chunkNums++;
	}

	public byte[] getPackageBuf() {
		return packageBuf;
	}

	public long getBlockId() {
		return blockId;
	}

	public void setReqId(long reqId) {
		this.reqId = reqId;
	}

	public int getOffsetInBlock() {
		return offsetInBlock;
	}

	public synchronized void writeData(byte[] inarray, int off, int len) {
		if (len == 0) {
			return;
		}
		System.arraycopy(inarray, off, packageBuf, dataEnd, len);
		dataEnd += len;
	}

	public synchronized void writeHead(byte[] inarray, int off, int len) {
		if (len == 0) {
			return;
		}
		System.arraycopy(inarray, off, packageBuf, checksumEnd, len);
		checksumEnd += len;
	}

	@Override
	public String toString() {
		return "DataPackage [reqId=" + reqId + ", maxChunks=" + maxChunks + ", packageByteNums=" + packageByteNums
				+ ", chunkNums=" + chunkNums + ", offsetInBlock="
				+ offsetInBlock + ", checksumEnd=" + checksumEnd + ", dataStart=" + dataStart + ", dataEnd=" + dataEnd
				+ ", blockId=" + blockId + "]";
	}
}
