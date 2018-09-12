package namenode.namespace;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import configuration.StorageConf;
import exception.ConfigurationException;
import exception.FSImageLoadException;
import namenode.block.Block;
import namenode.block.BlockInfo;
import namenode.block.BlocksManager;
import tools.FileFilter;
import tools.Sequence;

public class FSImage {
	private BlocksManager blocksManager;
	private INodeDirectory rootDir = null;
	private long readLineNum = 0;
	private static String IMAGE_DIR = StorageConf.getVal("namenode.fsimage.path", "/home/hadoop/xxytest/mydream/fsimage") + "/current";
	private static String IMAGE_TMP_DIR = StorageConf.getVal("namenode.fsimage.path", "/home/hadoop/xxytest/mydream/fsimage") + "/tmp";

	private Logger logger = Logger.getLogger(FSImage.class);

	public FSImage(BlocksManager blocksManager) {
		super();
		this.blocksManager = blocksManager;
	}

	public FSImage() {
		super();
	}

	public INodeDirectory getRootDir() {
		return rootDir;
	}

	public void setRootDir(INodeDirectory rootDir) {
		this.rootDir = rootDir;
	}

	public void loadFSImage() throws IOException {
		if (IMAGE_DIR == null) {
			throw new ConfigurationException("读取配置项image.path失败，该配置项设置数据在节点中存放的目录，必须设置");
		}
		File imageDirFile = new File(IMAGE_DIR);
		File imageTmpDirFile = new File(IMAGE_TMP_DIR);
		if (!imageDirFile.exists()) {
			imageDirFile.mkdirs();
		}
		if (!imageTmpDirFile.exists()) {
			imageTmpDirFile.mkdirs();
		}
		File[] files = imageDirFile.listFiles(new FileFilter("fsimage_[0-9]+$"));
		if (files == null || files.length == 0) {
			long id = Sequence.nextVal();
			rootDir = new INodeDirectory(id, "ROOT");
			return;
		}
		if (files.length != 1) {
			throw new FSImageLoadException("fsimage文件存在多个，读取异常");
		}
		File file = files[0];
		try (FileReader fr = new FileReader(file); BufferedReader br = new BufferedReader(fr);) {
			INodeDirectory nullNode = null;
			loadImageToNode(br, nullNode);
		} finally {

		}
	}

	public void loadImageToNode(BufferedReader br, INodeDirectory parentDirNode) throws IOException {
		String line = br.readLine();
		++readLineNum;
		String[] info;
		if (line == null || (info = line.split("\\|", -1)).length != 6) {
			throw new FSImageLoadException("fsimage文件在第" + readLineNum + "行格式错误，行为空或行列数不为6");
		}

		String type = info[0];
		long id = Long.parseLong(info[1]);
		String name = info[2];
		if ((!"0".equals(type) || !"ROOT".equals(name)) && readLineNum == 1) {
			throw new FSImageLoadException("fsimage文件在第一行格式错误，该行node的type不为0,或者名字不为ROOT");
		}
		if (!"0".equals(type) && parentDirNode != null && Long.parseLong(info[3]) != parentDirNode.getId()) {
			throw new FSImageLoadException("fsimage文件在第" + readLineNum + "行格式错误，该行记录的父id与前面记录读取的父id不相等");
		}
		String childsStr = info[4];
		String blocksStr = info[5];
		INode node = null;
		if ("1".equals(type)) {
			node = new INodeDirectory(id, name);
		} else if ("2".equals(type)) {
			node = new INodeFile(id, name);
		} else if ("0".equals(type)) {
			node = new INodeDirectory(id, name);
			rootDir = (INodeDirectory) node;
		}
		if (!"".equals(childsStr) && ("0".equals(type) || "1".equals(type))) {
			for (int i = 0; i < childsStr.split(",").length; i++) {
				loadImageToNode(br, (INodeDirectory) node);
			}
		} else if ("".equals(childsStr) && "2".equals(type)) {
			List<BlockInfo> blocks = new ArrayList<>();
			for (String blockStr : blocksStr.split(",")) {
				long blockId = Long.parseLong(blockStr);
				logger.info(String.format("node %s 的block %s", node.getId(), blockId));
				Block block = new Block(blockId);
				BlockInfo blockInfo = blocksManager.getBlockInfo(block);
				blocks.add(blockInfo);
				// BlockInfo blockInfo = new BlockInfo(blockId);
				blocks.add(blockInfo);
			}
			INodeFile fileNode = (INodeFile) node;
			fileNode.setBlocks(blocks);
		}
		if (parentDirNode != null) {
			parentDirNode.addChild(node);
			node.setParent(parentDirNode);
		}
	}

	public void printNode() throws IOException {
		if (rootDir == null) {
			return;
		}
		long imageId = Sequence.nextVal();
		String fileName = "fsimage_" + imageId;
		String imageNextFile = IMAGE_TMP_DIR + "/" + fileName;
		File nextImageFile = new File(imageNextFile);
		if (nextImageFile.exists())
			nextImageFile.delete();
		nextImageFile.createNewFile();
		FileWriter writer = new FileWriter(nextImageFile);
		writer.write(genRootStr(rootDir));
		writer.write("\r\n");
		printNode(writer, rootDir);
		writer.flush();
		writer.close();
		logger.info("tmpimagefile : " + imageNextFile + " has written");
		File imageDirFile = new File(IMAGE_DIR);
		File[] childFiles = imageDirFile.listFiles();
		for(File childFile : childFiles) {
			childFile.delete();
			logger.info("imagefile : " + childFile.getPath() + " has delete");
		}
		nextImageFile.renameTo(new File(IMAGE_DIR + "/" + fileName));
	}

	public void printNode(FileWriter writer, INode node) throws IOException {
		if (node.isDirectory()) {
			INodeDirectory dirNode = (INodeDirectory) node;
			if (dirNode.getChild().isEmpty()) {
				return;
			}
			List<INode> childsNode = dirNode.getChild();
			for (INode childNode : childsNode) {
				String nodeStr = genStr(childNode);
				writer.write(nodeStr);
				writer.write("\r\n");
				printNode(writer, childNode);
			}
		} else {
			return;
		}
	}

	public String genRootStr(INode node) {
		String result;
		String type = "0";
		String blockIdStr = "";
		String parentId = "";
		INodeDirectory dir = (INodeDirectory) node;
		List<INode> childs = dir.getChild();
		String childIdStr = "";
		if (!childs.isEmpty()) {
			for (INode child : childs) {
				childIdStr = childIdStr + child.getId() + ",";
			}
			childIdStr = childIdStr.substring(0, childIdStr.length() - 1);
		}
		result = type + "|" + node.getId() + "|" + node.getName() + "|" + parentId + "|" + childIdStr + "|"
				+ blockIdStr;
		return result;
	}

	public String genStr(INode node) {
		String result;
		String type;
		String childIdStr = "";
		String blockIdStr = "";
		String parentId = "";
		if (node.isDirectory()) {
			INodeDirectory dir = (INodeDirectory) node;
			List<INode> childs = dir.getChild();
			if (!childs.isEmpty()) {
				for (INode child : childs) {
					childIdStr = childIdStr + child.getId() + ",";
				}
				childIdStr = childIdStr.substring(0, childIdStr.length() - 1);
			}
			type = "1";
			parentId = String.valueOf(dir.getParent().getId());
		} else {
			INodeFile file = (INodeFile) node;
			type = "2";
			List<BlockInfo> blockInfos = file.getBlocks();
			if (!blockInfos.isEmpty()) {
				for (BlockInfo blockInfo : blockInfos) {
					blockIdStr = blockIdStr + blockInfo.getId() + ",";
				}
				blockIdStr = blockIdStr.substring(0, blockIdStr.length() - 1);
			}
			parentId = String.valueOf(file.getParent().getId());
		}
		result = type + "|" + node.getId() + "|" + node.getName() + "|" + parentId + "|" + childIdStr + "|"
				+ blockIdStr;
		return result;
	}
}
