package exception;

import java.io.IOException;

public class PathErrorException extends IOException {
	private static final long serialVersionUID = 1905142784319961415L;

	public PathErrorException() {
		super();
	}

	public PathErrorException(String s) {
		super(s);
	}
}
