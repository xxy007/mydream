package namenode.namespace;

import java.util.ArrayList;
import java.util.List;

import namenode.block.BlockInfo;

public class INodeFile extends INode {
	private List<BlockInfo> blocks;

	public INodeFile(long nodeId, String name) {
		super(null, nodeId, name);
		this.blocks = new ArrayList<>();
	}

	public INodeFile(INode parent, long nodeId, String name) {
		super(parent, nodeId, name);
		this.blocks = new ArrayList<>();
	}

	public INodeFile(INode parent, long nodeId, String name, List<BlockInfo> blocks) {
		super(parent, nodeId, name);
		this.blocks = blocks;
	}

	public List<BlockInfo> getBlocks() {
		return blocks;
	}

	public void setBlocks(List<BlockInfo> blocks) {
		this.blocks = blocks;
	}

	@Override
	public boolean isDirectory() {
		return false;
	}

	@Override
	public boolean isFile() {
		return true;
	}

	@Override
	public String toString() {
		String result = "";
		result = " INodeDirectory [ "
				+ " nodeId=" + nodeId
				+ " generationStamp=" + generationStamp 
				+ " name=" + name
				+ " blocks=" + blocks
				+ " parent=" + parent.getName()
				+ " ] ";
		return result; 
	}
}
