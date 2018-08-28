package com.trigger.cassandra.logger.settings;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

public class SettingsLoader {

	private static final String DEFAULT_PORT = "9042";
	private static final String DEFAULT_CREDS = "cassandra";
	private static final String DEFAULT_HOST = "127.0.0.1";
	private static final String DEFAULT_LOG_KEYSPACE = "product_v2";
	private static final String DEFAULT_LOG_TABLE = "product_by_maps";

	public static Settings load(String fileName) throws IOException {
		Properties properties = loadPropertiesFromClassPath(fileName);

		Settings settings = new Settings();
		settings.setLogKeyspace(normalize(properties.getProperty("logKeyspace", DEFAULT_LOG_KEYSPACE)));
		settings.setLogTable(normalize(properties.getProperty("logTable", DEFAULT_LOG_TABLE)));
		settings.setHost(normalize(properties.getProperty("host", DEFAULT_HOST)));
		settings.setPassword(normalize(properties.getProperty("password", DEFAULT_CREDS)));
		settings.setPort(Integer.parseInt(normalize(properties.getProperty("port", DEFAULT_PORT))));
		settings.setUsername(normalize(properties.getProperty("username", DEFAULT_CREDS)));
		return settings;
	}

	private static Properties loadPropertiesFromClassPath(String fileName) throws IOException {
		Properties properties = new Properties();

		try (InputStream stream = SettingsLoader.class.getClassLoader().getResourceAsStream(fileName)) {
			if (stream != null) {
				properties.load(stream);
			} else {
				throw new FileNotFoundException(fileName);
			}
		}

		return properties;
	}

	private static String normalize(String value) {
		return StringUtils.strip(value);

	}
}