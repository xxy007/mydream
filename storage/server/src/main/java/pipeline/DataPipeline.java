package pipeline;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class DataPipeline implements DataPipelineOperator {
	private Map<Long, Pipeline> pipelineInst = new ConcurrentHashMap<>();
	private Logger logger = Logger.getLogger(Pipeline.class);
	public DataPipeline() {
		super();
	}

	public synchronized PortInfo setUpPipeline(long blockId, boolean isLast) {
		logger.info("pre setup blockId is : " + blockId + " and pipelineInst is : " + pipelineInst);
		Pipeline pipeline = pipelineInst.get(blockId);
		if (pipeline == null) {
			pipeline = new Pipeline(blockId, isLast);
			PortInfo portInfo = pipeline.setUpPipeline();
			pipelineInst.put(blockId, pipeline);
			logger.info("after setup blockId is : " + blockId + " and pipelineInst is : " + pipelineInst);
			return portInfo;
		} else {
			pipelineInst.remove(blockId);
			logger.info("after setup blockId is : " + blockId + " and pipelineInst is : " + pipelineInst);
			return null;
		}
	}

	public boolean setPipelineInfo(long blockId, String preIp, String nextIp, int sendDataPort, int sendResponsePort) {
		logger.info("pre set info blockId is : " + blockId + " and pipelineInst is : " + pipelineInst);
		Pipeline pipeline = pipelineInst.get(blockId);
		if (pipeline != null) {
			pipeline.setPipelineInfo(preIp, nextIp, sendDataPort, sendResponsePort);
			pipelineInst.remove(blockId);
			logger.info("after set info blockId is : " + blockId + " and pipelineInst is : " + pipelineInst);
			return true;
		} else {
			logger.info("after set info blockId is : " + blockId + " and pipelineInst is : " + pipelineInst);
			return false;
		}
	}
}
