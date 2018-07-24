package configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public final class StorageConf {
	private static Logger logger = Logger.getLogger(StorageConf.class);
	private static volatile Properties properties;

	static {
		properties = new Properties();
		ClassLoader loader = StorageConf.class.getClassLoader();
		InputStream in = loader.getResourceAsStream("storage-site.xml");
		try {
			properties.load(in);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	public static String getVal(String key) {
		return properties.getProperty(key, null);
	}
	
	public static String getVal(String key, String defaultVal) {
		return properties.getProperty(key, defaultVal);
	}
	
	public static void setVal(String key, String val) {
		properties.setProperty(key, val);
	}
}
