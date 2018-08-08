package com.trigger.cassandra.logger.settings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

import com.trigger.cassandra.logger.settings.Settings;
import com.trigger.cassandra.logger.settings.SettingsProvider;

public class SettingsProviderTest {

	@Test
	public void loadDefaultSettingsIfUserSettingsFileNotFound() {
		Settings settings = SettingsProvider.getSettings();
		assertThat(settings.getLogKeyspace(), is("product_v2_dev"));
		assertThat(settings.getLogTable(), is("product_by_timestamps"));
	}
}