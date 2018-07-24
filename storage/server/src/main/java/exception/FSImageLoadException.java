package exception;

import java.io.IOException;

public class FSImageLoadException extends IOException {
	private static final long serialVersionUID = 7066848170420114608L;

	public FSImageLoadException() {
		super();
	}

	public FSImageLoadException(String msg) {
		super(msg);
	}
}