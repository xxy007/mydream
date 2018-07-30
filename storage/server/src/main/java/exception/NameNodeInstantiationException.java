package exception;

import java.io.IOException;

public class NameNodeInstantiationException extends IOException {
	private static final long serialVersionUID = 7066848170420114608L;

	public NameNodeInstantiationException() {
		super();
	}

	public NameNodeInstantiationException(String msg) {
		super(msg);
	}
}