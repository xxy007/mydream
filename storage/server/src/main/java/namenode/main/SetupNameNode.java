package namenode.main;

import java.io.IOException;

import namenode.block.BlocksManager;
import namenode.namespace.FSImage;
import namenode.namespace.INodeDirectory;

public class SetupNameNode {
	public static void main(String[] args) throws IOException {
		long totalMem = Runtime.getRuntime().totalMemory();
		int blocksSize = (int) (totalMem*0.02);
		BlocksManager BlocksManager = new BlocksManager(blocksSize);
		//通过netty获取datanode返回的block信息，然后写入manager
		FSImage fsImage = new FSImage();
		fsImage.loadFSImage();
		INodeDirectory rootDir = fsImage.getRootDir();
		//这样，namenode内存中就存在了目录信息和block信息，此时开启rpc，接收客户端的请求
	}
}
