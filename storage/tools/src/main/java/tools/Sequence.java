package tools;

import org.apache.log4j.Logger;

public class Sequence {
	static String querySeqSql = "select seq_ngbilling.nextval";
	private static Logger logger = Logger.getLogger(Sequence.class);
	public static long nextVal() {
		return Long.parseLong(String.format("3%d%s", System.currentTimeMillis(), KeyGen.uuid(4, 0, 9)));
	}
	public static void main(String[] args) {
		logger.info(nextVal());
		logger.info(Long.MAX_VALUE);
	}
}