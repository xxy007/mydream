package pipeline;

public interface DataPipelineOperator {
	void setUpPipeline(long blockId, String preIp, String nextIp, int dataPort, int responsePort,
			boolean isLast);
}
