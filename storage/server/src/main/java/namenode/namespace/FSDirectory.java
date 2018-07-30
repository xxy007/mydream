package namenode.namespace;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import exception.PathErrorException;
import exception.PathNotFoundException;
import namenode.block.BlockInfo;
import tools.Sequence;

public class FSDirectory implements FSOperator{
	private final INodeDirectory rootDir; // 没有name,parent属性，只有child，相当于根目录
	private Logger logger = Logger.getLogger(FSDirectory.class);
	private final ReentrantReadWriteLock dirLock;

	public FSDirectory(INodeDirectory rootDir) {
		this.rootDir = rootDir;
		this.dirLock = new ReentrantReadWriteLock(true);
		logger.info("rootDir is " + rootDir);
	}

	public List<BlockInfo> getBlockInfo(String filePath) throws IOException {
		INode node = findNode(filePath, true);
		INodeFile nodeFile = (INodeFile) node;
		return nodeFile.getBlocks();
	}

	public boolean createDir(String dirPath) throws IOException {
		dirLock.writeLock().lock();
		boolean result = createNode(dirPath, false);
		dirLock.writeLock().unlock();
		return result;
	}

	public boolean createFile(String filePath) throws IOException {
		dirLock.writeLock().lock();
		boolean result = createNode(filePath, true);
		dirLock.writeLock().unlock();
		return result;
	}

	private boolean createNode(String path, boolean isFile) throws IOException {
		File file = new File(path);
		String parent = file.getParent();
		INodeDirectory parentNode = (INodeDirectory) findNode(parent, false);
		logger.info("parentNode is " + parentNode);
		String name = file.getName();
		long nodeId = Sequence.nextVal();
		INode node;
		if (isFile) {
			node = new INodeFile(parentNode, nodeId, name);
		} else {
			node = new INodeDirectory(parentNode, nodeId, name);
		}
		parentNode.addChild(node);
		logger.info("after add child parentNode is " + parentNode);
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
			boolean isFindNode = false;
			for (INode node : child) {
				logger.info("node is " + node.getName());
				logger.info("components[" + count+ "] is " +  components[count]);
				logger.info("componentNum is " +  componentNum);
				logger.info("isFile is " +  isFile);
				if (components[count].equals(node.getName()) && count != componentNum - 1) {
					curNode = (INodeDirectory) node;
					count++;
					logger.info("curNode is " +  curNode.toString());
					isFindNode = true;
					break;
				} else if (components[count].equals(node.getName()) && count == componentNum - 1) {
					logger.info("2: isFile is " + isFile);
					logger.info("node.isDirectory() " + node.isDirectory());
					logger.info("2: components[count] is " +  components[count]);
					if ((isFile && node.isFile()) || (!isFile && node.isDirectory())) {
						return node;
					}
				}
			}
			if(!isFindNode)
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
				components[componentNum] = components[i];
				++componentNum;
			}
		}
		String[] result = new String[componentNum];
		System.arraycopy(components, 0, result, 0, componentNum);
		return result;
	}
}
