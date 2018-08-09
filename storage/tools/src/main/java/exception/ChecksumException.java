package exception;

import java.io.IOException;
/**
 * Thrown when bytesPerChecksun field in the meta file is less than
 * or equal to 0 or type is invalid.
 **/
public class ChecksumException extends IOException {

  private static final long serialVersionUID = 1L;

  public ChecksumException(String s) {
    super(s);
  }
}
