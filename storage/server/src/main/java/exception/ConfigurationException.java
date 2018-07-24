package exception;

import java.io.IOException;

public class ConfigurationException extends IOException {
	private static final long serialVersionUID = 7066848170420114608L;

	public ConfigurationException() {
		super();
	}

	public ConfigurationException(String msg) {
		super(msg);
	}
}