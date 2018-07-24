package tools;

public class Sequence {
	static String querySeqSql = "select seq_ngbilling.nextval";

	public static long nextVal() {
		return Long.parseLong(String.format("3%d%s", System.currentTimeMillis(), KeyGen.uuid(4, 0, 9)));
	}
	public static void main(String[] args) {
		System.out.println(nextVal());
		System.out.println(Long.MAX_VALUE);
	}
}