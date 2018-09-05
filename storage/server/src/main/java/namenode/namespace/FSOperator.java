package namenode.namespace;

import java.util.List;
import datanode.storage.DataNodeStorage;
import namenode.block.BlockInfo;

public interface FSOperator {
	List<BlockInfo> getBlockInfo(String filePath);

	boolean createDir(String dirPath);

	boolean createFile(String filePath);
	
	void addBlockInfo(String filePath, BlockInfo blockInfo);
	
	List<DataNodeStorage> getDataNode();
}
