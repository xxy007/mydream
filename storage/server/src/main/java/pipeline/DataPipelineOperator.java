package pipeline;

public interface DataPipelineOperator {
	PortInfo setUpPipeline(long blockId, boolean isLast);
	boolean setPipelineInfo(long blockId, String preIp, String nextIp, int sendDataPort, int sendResponsePort);
}
