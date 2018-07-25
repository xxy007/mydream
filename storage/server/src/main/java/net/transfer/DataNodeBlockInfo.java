package net.transfer;

import java.io.Serializable;

public class DataNodeBlockInfo implements Serializable {

	private static final long serialVersionUID = -3220897044021882720L;

	private long blockId;
	private long blockNumBytes;
	public long getBlockId() {
		return blockId;
	}
	public void setBlockId(long blockId) {
		this.blockId = blockId;
	}
	public long getBlockNumBytes() {
		return blockNumBytes;
	}
	public void setBlockNumBytes(long blockNumBytes) {
		this.blockNumBytes = blockNumBytes;
	}
	@Override
	public String toString() {
		return "DataNodeReport [blockId=" + blockId + ", blockNumBytes=" + blockNumBytes + "]";
	}
}
