package crc32;

import java.util.zip.CRC32;

import tools.ByteObject;

public class MyCrc {
	public static byte[] get4ByteCrc32(byte[] src) {
		CRC32 crc32 = new CRC32();
		crc32.update(src);
		int crc = (int)crc32.getValue();
		return ByteObject.intToByteArray(crc);
	}
	
	public static byte[] get4ByteCrc32(byte[] src, int offset, int len) {
		CRC32 crc32 = new CRC32();
		crc32.update(src, offset, len);
		int crc = (int)crc32.getValue();
		return ByteObject.intToByteArray(crc);
	}
	
	public static int getCrc32(byte[] src, int off, int len) {
		CRC32 crc32 = new CRC32();
		crc32.update(src, off, len);
		return (int)crc32.getValue();
	}

	public static int getCrc32(byte[] src) {
		CRC32 crc32 = new CRC32();
		crc32.update(src);
		return (int)crc32.getValue();
	}
}
