package exception;

import java.io.IOException;

public class PathNotFoundException extends IOException {
	private static final long serialVersionUID = 9121210214666077666L;

	public PathNotFoundException() {
		super();
	}

	public PathNotFoundException(String s) {
		super(s);
	}

	private PathNotFoundException(String path, String reason) {
		super(path + ((reason == null) ? "" : " (" + reason + ")"));
	}
}
