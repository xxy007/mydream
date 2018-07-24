package tools;

import java.io.File;

public class FileInfo {
	public static long getFileSize(File file, FileSizeUnit unit) {
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
	public static void main(String[] args) {
		File file = new File("C://aep-billing-portal-1.0.0-alogic.war");
		System.out.println(getFileSize(file, FileSizeUnit.B));
	}
}
