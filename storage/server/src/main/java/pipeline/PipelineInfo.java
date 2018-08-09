package pipeline;

public class PipelineInfo {
	private long blockId;
	private PipelineInfo srcInfo;
	private String thisIp;
	private int thisDataPort;
	private int thisResponsePort;
	private String srcIp;
	private PipelineInfo nextInfo;
	private boolean isLast;
	
	public long getBlockId() {
		return blockId;
	}
	public void setBlockId(long blockId) {
		this.blockId = blockId;
	}
	public PipelineInfo getSrcInfo() {
		return srcInfo;
	}
	public void setSrcInfo(PipelineInfo srcInfo) {
		this.srcInfo = srcInfo;
	}
	public String getThisIp() {
		return thisIp;
	}
	public void setThisIp(String thisIp) {
		this.thisIp = thisIp;
	}
	public int getThisDataPort() {
		return thisDataPort;
	}
	public void setThisDataPort(int thisDataPort) {
		this.thisDataPort = thisDataPort;
	}
	public int getThisResponsePort() {
		return thisResponsePort;
	}
	public void setThisResponsePort(int thisResponsePort) {
		this.thisResponsePort = thisResponsePort;
	}
	public String getSrcIp() {
		return srcIp;
	}
	public void setSrcIp(String srcIp) {
		this.srcIp = srcIp;
	}
	public PipelineInfo getNextInfo() {
		return nextInfo;
	}
	public void setNextInfo(PipelineInfo nextInfo) {
		this.nextInfo = nextInfo;
	}
	public boolean isLast() {
		return isLast;
	}
	public void setLast(boolean isLast) {
		this.isLast = isLast;
	}
}
