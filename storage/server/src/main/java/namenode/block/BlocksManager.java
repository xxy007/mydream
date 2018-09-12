package namenode.block;

import org.apache.log4j.Logger;
import tools.LightSet;
import tools.XSet;

public class BlocksManager {
	
	/**
	 * blockID与blockInfo的映射map
	 */
	private XSet<Block, BlockInfo> blocks;
	private static Logger logger = Logger.getLogger(BlocksManager.class);
	public BlocksManager() {
		int capacity = LightSet.computeCapacity(2);
		blocks = new LightSet<Block, BlockInfo>(capacity);
	}
	
	public BlockInfo getBlockInfo(Block block) {
		return blocks.get(block);
	}
	
	public void putBlockInfo(BlockInfo blockInfo)
	{
		blocks.put(blockInfo);
	}
}
