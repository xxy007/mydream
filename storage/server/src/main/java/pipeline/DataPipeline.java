package pipeline;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataPipeline implements DataPipelineOperator {
	private Map<Long, Pipeline> pipelineInst = new ConcurrentHashMap<>(); 
	public synchronized PortInfo setUpPipeline(long blockId, boolean isLast) {
		Pipeline pipeline = pipelineInst.get(blockId);
		if(pipeline == null) {
			pipeline = new Pipeline(blockId, isLast);
			PortInfo portInfo = pipeline.setUpPipeline();
			pipelineInst.put(blockId, pipeline);
			return portInfo;
		}else {
			return null;
		}
	}
	public boolean setPipelineInfo(long blockId, String preIp, String nextIp, int sendDataPort, int sendResponsePort) {
		Pipeline pipeline = pipelineInst.get(blockId);
		if(pipeline != null) {
			pipeline.setPipelineInfo(preIp, nextIp, sendDataPort, sendResponsePort);
			pipelineInst.remove(blockId);
			return true;
		}else {
			return false;
		}
	}
}
