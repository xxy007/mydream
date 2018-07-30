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
	
	public static void main(String[] args) {
		BlocksManager ma = new BlocksManager();
		BlockInfo blockInfo1 = new BlockInfo(1, 1000, 1);
		BlockInfo blockInfo2 = new BlockInfo(2, 1000, 2);
		BlockInfo blockInfo3 = new BlockInfo(3, 1000, 3);
		BlockInfo blockInfo4 = new BlockInfo(4, 1000, 4);
		BlockInfo blockInfo5 = new BlockInfo(5, 1000, 5);

		ma.putBlockInfo(blockInfo1);
		ma.putBlockInfo(blockInfo2);
		ma.putBlockInfo(blockInfo3);
		ma.putBlockInfo(blockInfo4);
		ma.putBlockInfo(blockInfo5);
		
		BlockInfo blockInfo6 = new BlockInfo(1, 1000, 1);
		
		logger.info(ma.getBlockInfo(blockInfo6));
		
	}
}
