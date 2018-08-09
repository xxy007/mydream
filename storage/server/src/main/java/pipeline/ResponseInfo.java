package pipeline;

import java.io.Serializable;

public class ResponseInfo implements Serializable{
	private long reqId;
	private String reason;
	private int resultCode;

	/**
	 * 
	 */
	private static final long serialVersionUID = 5471697719595706460L;

	public long getReqId() {
		return reqId;
	}

	public void setReqId(long reqId) {
		this.reqId = reqId;
	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public int getResultCode() {
		return resultCode;
	}

	public void setResultCode(int resultCode) {
		this.resultCode = resultCode;
	}

	@Override
	public String toString() {
		return "ResponseInfo [reqId=" + reqId + ", reason=" + reason + ", resultCode=" + resultCode + "]";
	}
}
