package pipeline;

public class DataPipeline implements DataPipelineOperator {

	public void setUpPipeline(long blockId, String preIp, String nextIp, int dataPort, int responsePort,
			boolean isLast) {
		Pipeline pipeline = new Pipeline(blockId, nextIp, nextIp, responsePort, responsePort, isLast);
		pipeline.setUpPipeline();
	}
}
