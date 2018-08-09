package client.io;

import java.io.IOException;
import java.io.OutputStream;

import crc32.MyCrc;

public abstract class MyDreamOutputStream extends OutputStream {

	private byte[] chunkBuf;
	private int count;

	protected MyDreamOutputStream(int chunkByteNum) {
		chunkBuf = new byte[chunkByteNum];
		count = 0;
	}

	protected abstract void writeChunk(byte[] b, int bOffset, int bLen, byte[] checksum, int checksumOffset,
			int checksumLen) throws IOException;

	@Override
	public synchronized void write(int b) throws IOException {
		chunkBuf[count++] = (byte) b;
		if (count == chunkBuf.length) {
			writeChecksumChunks(chunkBuf, 0, chunkBuf.length);
		}
	}

	@Override
	public synchronized void write(byte b[], int off, int len) throws IOException {
		if (off < 0 || len < 0 || off > b.length - len) {
			throw new ArrayIndexOutOfBoundsException();
		}
		
		for (int n = 0; n < len; n += write1(b, off + n, len - n)) {
		}
	}

	private int write1(byte b[], int off, int len) throws IOException {
		if (count == 0 && len >= chunkBuf.length) {
			final int length = chunkBuf.length;
			writeChecksumChunks(b, off, length);
			return length;
		}

		int bytesToCopy = chunkBuf.length - count;
		bytesToCopy = (len < bytesToCopy) ? len : bytesToCopy;
		System.arraycopy(b, off, chunkBuf, count, bytesToCopy);
		count += bytesToCopy;
		if (count == chunkBuf.length) {
			writeChecksumChunks(chunkBuf, 0, chunkBuf.length);
		}
		return bytesToCopy;
	}

	@Override
	public void flush() throws IOException {
		super.flush();
		writeChecksumChunks(chunkBuf, 0, count);
	}
	
	protected synchronized int getBufferedDataSize() {
		return count;
	}

	private void writeChecksumChunks(byte b[], int off, int len) throws IOException {
		byte[] checksum = MyCrc.get4ByteCrc32(b, off, len);
		writeChunk(b, off, len, checksum, 0, checksum.length);
	}
}
