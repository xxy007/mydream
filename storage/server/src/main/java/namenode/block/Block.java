package namenode.block;

import tools.LinkedElement;

public class Block implements Comparable<Block>, Cloneable, LinkedElement {
	private long blockId;
	private long numBytes;
	private long generationStamp;
	private LinkedElement next;

	public Block(long blockId, long numBytes) {
		this.blockId = blockId;
		this.numBytes = numBytes;
	}

	public Block(long blockId) {
		this(blockId, 0);
	}

	/**
	 * block的唯一标识id
	 * 
	 * @return
	 */
	public long getId() {
		return blockId;
	}

	long getBlockNumBytes() {
		return numBytes;
	}

	long getGenerationStamp() {
		return generationStamp;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Block) {
			Block block = (Block) obj;
			return (block.getId() == this.getId());
		}
		return false;
	}

	@Override
	public int compareTo(Block o) {
		if (this.getId() < o.getId())
			return -1;
		else if (this.getId() < o.getId()) {
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public int hashCode() {
		return (int) (blockId ^ (blockId >>> 32));
	}

	@Override
	protected Block clone() throws CloneNotSupportedException {
		return (Block) super.clone();
	}

	public void setNext(LinkedElement next) {
		this.next = next;
	}

	public LinkedElement getNext() {
		return next;
	}

}
