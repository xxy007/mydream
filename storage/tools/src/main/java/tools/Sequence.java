package tools;

public class Sequence {
	public static long nextVal() {
		return Long.parseLong(String.format("3%d%s", System.currentTimeMillis(), KeyGen.uuid(4, 0, 9)));
	}
}