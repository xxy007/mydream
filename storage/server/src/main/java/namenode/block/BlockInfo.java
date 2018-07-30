package namenode.block;

import java.util.ArrayList;
import java.util.List;

import datanode.storage.DataNodeStorage;
import tools.LinkedElement;

public class BlockInfo extends Block implements LinkedElement {

	private int replication;
	private List<DataNodeStorage> dataNodeList = new ArrayList<>();

	public BlockInfo(long blockId) {
		super(blockId, 0);
		this.replication = 3;
	}
	
	public BlockInfo(long blockId, long numBytes) {
		super(blockId, numBytes);
		this.replication = 3;
	}
	
	public BlockInfo(long blockId, long numBytes, int replication) {
		super(blockId, numBytes);
		this.replication = replication;
	}

	public void addDataNodeStorage(DataNodeStorage dataNodeStorage) {
		dataNodeList.add(dataNodeStorage);
	}
	
	@Override
	public String toString() {
		return "BlockInfo [blockId=" + super.getId() + ", numBytes=" + super.getBlockNumBytes() + ", replication="
				+ replication + ", dataNodeList=" + dataNodeList + "]";
	}
}
