package tools;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileFilter implements FilenameFilter {
	private String regex;

	public FileFilter(String regex) {
		this.regex = regex;
	}

	public boolean accept(File dir, String name) {
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(name);
		return m.find();
	}
}