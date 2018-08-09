package exception;

import java.io.IOException;
/**
 * Thrown when bytesPerChecksun field in the meta file is less than
 * or equal to 0 or type is invalid.
 **/
public class PipelineException extends IOException {

  private static final long serialVersionUID = 1L;

  public PipelineException(String s) {
    super(s);
  }
}
