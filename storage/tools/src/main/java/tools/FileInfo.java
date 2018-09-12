package tools;

import java.io.File;

import org.apache.log4j.Logger;

public class FileInfo {
	private static Logger logger = Logger.getLogger(FileInfo.class);
	public static long getFileSize(File file, CapacityUnit unit) {
		long fileSize = file.length();
		long result = 0;
		switch(unit) {
		case B:
			result = fileSize;
			break;
		case KB:
			result = fileSize/1024;
			break;
		case MB:
			result = fileSize/(1024*1024);
			break;
		case GB:
			result = fileSize/(1024*1024*1024);
			break;
		default:
				
		}
		return result;
	}
}
