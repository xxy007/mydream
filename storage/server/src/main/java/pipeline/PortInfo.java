package pipeline;

public class PortInfo {
	private int receiveDataPort;
	private int receiveResponsePort;
	public PortInfo(int receiveDataPort, int receiveResponsePort) {
		super();
		this.receiveDataPort = receiveDataPort;
		this.receiveResponsePort = receiveResponsePort;
	}
	public int getReceiveDataPort() {
		return receiveDataPort;
	}
	public void setReceiveDataPort(int receiveDataPort) {
		this.receiveDataPort = receiveDataPort;
	}
	public int getReceiveResponsePort() {
		return receiveResponsePort;
	}
	public void setReceiveResponsePort(int receiveResponsePort) {
		this.receiveResponsePort = receiveResponsePort;
	}
	@Override
	public String toString() {
		return "PortInfo [receiveDataPort=" + receiveDataPort + ", receiveResponsePort=" + receiveResponsePort + "]";
	}
}
