package exception;

import java.io.IOException;

public class ParentNotDirectoryException extends IOException {
	private static final long serialVersionUID = 7066848170420114608L;

	public ParentNotDirectoryException() {
		super();
	}

	public ParentNotDirectoryException(String msg) {
		super(msg);
	}
}