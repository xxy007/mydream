package pipeline;

import java.io.Serializable;

public class PortInfo implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8838524182498854601L;
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
