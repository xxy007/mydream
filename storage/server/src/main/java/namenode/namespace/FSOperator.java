package namenode.namespace;

import java.io.IOException;
import java.util.List;
import namenode.block.BlockInfo;

public interface FSOperator {
	public List<BlockInfo> getBlockInfo(String filePath) throws IOException;

	public boolean createDir(String dirPath) throws IOException;

	public boolean createFile(String filePath) throws IOException;
}
