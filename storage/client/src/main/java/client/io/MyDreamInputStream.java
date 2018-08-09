package client.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public abstract class MyDreamInputStream extends InputStream implements Seekable, PositionedReadable{

	  /**
	   * Seek to the given offset from the start of the file.
	   * The next read() will be from that location.  Can't
	   * seek past the end of the file.
	   */
	  @Override
	  public abstract void seek(long pos) throws IOException;

	  /**
	   * Return the current offset from the start of the file
	   */
	  @Override
	  public abstract long getPos() throws IOException;

	  /**
	   * Seeks a different copy of the data.  Returns true if 
	   * found a new source, false otherwise.
	   */
	  @Override
	  public abstract boolean seekToNewSource(long targetPos) throws IOException;

	  @Override
	  public int read(long position, byte[] buffer, int offset, int length)
	    throws IOException {
	    synchronized (this) {
	      long oldPos = getPos();
	      int nread = -1;
	      try {
	        seek(position);
	        nread = read(buffer, offset, length);
	      } finally {
	        seek(oldPos);
	      }
	      return nread;
	    }
	  }
	    
	  @Override
	  public void readFully(long position, byte[] buffer, int offset, int length)
	    throws IOException {
	    int nread = 0;
	    while (nread < length) {
	      int nbytes = read(position+nread, buffer, offset+nread, length-nread);
	      if (nbytes < 0) {
	        throw new EOFException("End of file reached before reading fully.");
	      }
	      nread += nbytes;
	    }
	  }
	    
	  @Override
	  public void readFully(long position, byte[] buffer)
	    throws IOException {
	    readFully(position, buffer, 0, buffer.length);
	  }
}
