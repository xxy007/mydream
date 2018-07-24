package namenode.namespace;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.log4j.Logger;

import exception.PathErrorException;
import exception.PathNotFoundException;
import namenode.block.BlockInfo;
import tools.Sequence;

public class FSDirectory implements FSOperator{
	private INodeDirectory rootDir; // 没有name,parent属性，只有child，相当于根目录
	private Logger logger = Logger.getLogger(FSDirectory.class);

	public FSDirectory(INodeDirectory rootDir) {
		this.rootDir = rootDir;
	}

	public List<BlockInfo> getBlockInfo(String filePath) throws IOException {
		INode node = findNode(filePath, true);
		INodeFile nodeFile = (INodeFile) node;
		return nodeFile.getBlocks();
	}

	public boolean createDir(String dirPath) throws IOException {
		return createNode(dirPath, false);
	}

	public boolean createFile(String filePath) throws IOException {
		return createNode(filePath, true);
	}

	public boolean createNode(String filePath, boolean isFile) throws IOException {
		File file = new File(filePath);
		String parent = file.getParent();
		INodeDirectory parentNode = (INodeDirectory) findNode(parent, false);
		String name = file.getName();
		long nodeId = Sequence.nextVal();
		INode node;
		if (isFile) {
			node = new INodeFile(parentNode, nodeId, name);
		} else {
			node = new INodeDirectory(parentNode, nodeId, name);
		}
		parentNode.addChild(node);
		return true;
	}

	private INode findNode(String path, boolean isFile) throws IOException {
		String[] components = getValidPath(path);
		int componentNum = components.length;
		if (componentNum == 0) {
			if (!isFile) {
				return rootDir;
			}
			throw new PathNotFoundException("this path : " + path + "is not found");
		}
		int count = 0;
		INodeDirectory curNode = rootDir;
		while (curNode != null && curNode.isDirectory() && count < componentNum) {
			List<INode> child = curNode.getChild();
			for (INode node : child) {
				if (components[count].equals(node.getName()) && count != componentNum - 1) {
					curNode = (INodeDirectory) node;
					count++;
					continue;
				} else if (components[count].equals(node.getName()) && count == componentNum - 1) {
					if ((isFile && node.isFile()) || (!isFile && node.isDirectory())) {
						return node;
					}
				}
			}
			break;
		}
		throw new PathNotFoundException("this path : " + "is not found");
	}

	private static String[] getValidPath(String path) throws IOException {
		if (path == null || "".equals(path) || path.charAt(0) != File.separator.charAt(0)) {
			throw new PathErrorException("path : " + path + "is error");
		}
		File file = new File(path);
		String validPath = file.getPath();

		String[] components = validPath.split("\\" + File.separator);
		int length = components.length;
		if (length == 0) {
			return components;
		}
		int componentNum = 0;
		for (int i = 0; i < length; i++) {
			if (!"".equals(components[i])) {
				components[componentNum] = File.separator + components[i];
				++componentNum;
			}
		}
		String[] result = new String[componentNum];
		System.arraycopy(components, 0, result, 0, componentNum);
		return result;
	}

	public static void main(String[] args) throws IOException {
		// String a = "/aaa/bbb/bbb/ccc///ddd";
		// File file = new File(a);
		// System.out.println(file.getParent());
		// System.out.println(file.getName());
		// String[] components = a.split("/");
		// System.out.println(Arrays.asList(components));
		// System.out.println(components.length);
		// String[] coms = getValidPath("///");

		// String path = File.separator + File.separator + File.separator + "aaa";
		// File file = new File(path);
		// path = file.getPath();
		// System.out.println(path);
		// String[] coms = path.split("\\" + File.separator);
		//
		// System.out.println(coms == null);
		// System.out.println(coms.length);
		// System.out.println(Arrays.asList(coms));

		String path = "/aaa";
		System.out.println(path.charAt(0) == '/');
	}
}
