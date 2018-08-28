package com.trigger.cassandra.logger.settings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;

import org.junit.Test;

import com.trigger.cassandra.logger.settings.Settings;
import com.trigger.cassandra.logger.settings.SettingsLoader;

public class SettingsLoaderTest {

	@Test
	public void loadValidSettings() throws IOException {
		Settings settings = SettingsLoader.load("com/trigger/cassandra/logger/settings/ValidSettings.properties");
		assertThat(settings.getLogKeyspace(), is("test_logger"));
		assertThat(settings.getLogTable(), is("test_log"));
	}

	@Test
	public void useDefaultLogTableIfNotProvidedInSettingsFile() throws IOException {
		Settings settings = SettingsLoader
				.load("com/trigger/cassandra/logger/settings/SettingsWithoutLogTable.properties");
		assertThat(settings.getLogTable(), is("product_by_maps"));
	}

	@Test
	public void useDefaultLogKeyspaceIfNotProvidedInSettingsFile() throws IOException {
		Settings settings = SettingsLoader
				.load("com/trigger/cassandra/logger/settings/SettingsWithoutLogKeyspace.properties");
		assertThat(settings.getLogKeyspace(), is("product_v2"));
	}

	@Test(expected = IOException.class)
	public void failIfNoSettingsFileFound() throws IOException {
		SettingsLoader.load("Nonexistent.properties");
	}
}