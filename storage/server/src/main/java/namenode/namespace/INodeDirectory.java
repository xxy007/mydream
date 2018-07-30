package namenode.namespace;

import java.util.ArrayList;
import java.util.List;

public class INodeDirectory extends INode {

	private List<INode> childNode;

	public INodeDirectory(long nodeId, String name) {
		super(null, nodeId, name);
		this.childNode = new ArrayList<>();
	}

	public INodeDirectory(INode parent, long nodeId, String name) {
		super(parent, nodeId, name);
		this.childNode = new ArrayList<>();
	}

	public INodeDirectory(INode parent, long nodeId, String name, List<INode> childNode) {
		super(parent, nodeId, name);
		this.childNode = childNode;
	}

	public void addChild(INode node) {
		childNode.add(node);
	}

	public List<INode> getChild() {
		return childNode;
	}

	public void removeChild(INode node) {
		childNode.remove(node);
	}

	@Override
	public boolean isDirectory() {
		return true;
	}

	@Override
	public boolean isFile() {
		return false;
	}

	@Override
	public String toString() {
		String result = "";
		result = " INodeDirectory [ "
				+ " nodeId=" + nodeId
				+ " generationStamp=" + generationStamp
				+ " name=" + name
				+ " childNode" + childNode;
		if(super.getParent() != null) {
			result = result + " parent=" + parent.getName();
		}
		result = result + " ] ";
		return result; 
	}
}
