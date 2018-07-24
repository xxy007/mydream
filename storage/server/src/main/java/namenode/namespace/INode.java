package namenode.namespace;

import tools.LinkedElement;

public abstract class INode implements Comparable<INode>, LinkedElement {
	private INode parent;
	private long nodeId;
	private long generationStamp;
	private LinkedElement next;
	private String name;

	public INode(long nodeId, String name) {
		this(null, nodeId, null, name);
	}

	public INode(INode parent, long nodeId, String name) {
		this(parent, nodeId, null, name);
	}

	public INode(INode parent, long nodeId, LinkedElement next, String name) {
		this.parent = parent;
		this.nodeId = nodeId;
		this.next = next;
		this.name = name;
		generationStamp = System.currentTimeMillis();
	}

	public INode getParent() {
		return parent;
	}

	public void setParent(INode parent) {
		this.parent = parent;
	}

	public long getId() {
		return nodeId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getGenerationStamp() {
		return generationStamp;
	}

	public void setGenerationStamp(long generationStamp) {
		this.generationStamp = generationStamp;
	}

	public abstract boolean isFile();

	public abstract boolean isDirectory();

	@Override
	public void setNext(LinkedElement next) {
		this.next = next.getNext();
	}

	@Override
	public LinkedElement getNext() {
		return next;
	}

	@Override
	public int compareTo(INode o) {
		if (this.getId() < o.getId())
			return -1;
		else if (this.getId() < o.getId()) {
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof INode) {
			INode node = (INode) obj;
			return (node.getId() == this.getId());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return (int) (nodeId ^ (nodeId >>> 32));
	}

	@Override
	protected INode clone() throws CloneNotSupportedException {
		return (INode) super.clone();
	}
}
