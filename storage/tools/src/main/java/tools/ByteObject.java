package tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ByteObject {
	public static Object getObject(byte[] bytes) {
		Object obj = null;
		try (ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
				ObjectInputStream oi = new ObjectInputStream(bi);) {
			obj = oi.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return obj;
	}

	public static byte[] getBytes(Object obj) {
		byte[] bytes = null;
		try (ByteArrayOutputStream bo = new ByteArrayOutputStream();
				ObjectOutputStream oo = new ObjectOutputStream(bo);) {
			oo.writeObject(obj);
			bytes = bo.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bytes;
	}

	public static byte[] intToByteArray(int i) {
		byte[] result = new byte[4];
		result[0] = (byte) ((i >> 24) & 0xFF);
		result[1] = (byte) ((i >> 16) & 0xFF);
		result[2] = (byte) ((i >> 8) & 0xFF);
		result[3] = (byte) (i & 0xFF);
		return result;
	}

	public static int byteArrayToInt(byte[] b) {
		byte[] a = new byte[4];
		int i = a.length - 1, j = b.length - 1;
		for (; i >= 0; i--, j--) {
			if (j >= 0)
				a[i] = b[j];
			else
				a[i] = 0;
		}
		int v0 = (a[0] & 0xff) << 24;
		int v1 = (a[1] & 0xff) << 16;
		int v2 = (a[2] & 0xff) << 8;
		int v3 = (a[3] & 0xff);
		return v0 + v1 + v2 + v3;
	}
	
	public static int byteArrayToInt(byte[] b, int off, int len) {
		byte[] a = new byte[4];
		int i = a.length - 1, j = len - 1;
		for (; i >= 0; i--, j--) {
			if (j >= 0)
				a[i] = b[off + j];
			else
				a[i] = 0;
		}
		int v0 = (a[0] & 0xff) << 24;
		int v1 = (a[1] & 0xff) << 16;
		int v2 = (a[2] & 0xff) << 8;
		int v3 = (a[3] & 0xff);
		return v0 + v1 + v2 + v3;
	}
	
}
