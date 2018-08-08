package com.trigger.cassandra.logger.store;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.trigger.cassandra.logger.log.LogEntry;
import com.trigger.cassandra.logger.log.Operation;
import com.trigger.cassandra.logger.settings.Settings;
import com.trigger.cassandra.logger.settings.SettingsProvider;
import com.trigger.cassandra.logger.store.LogEntryStore;

public class LogEntryStoreIntegrationTest {

	LogEntryStore store;

	@Before
	public void setup() {
		Settings settings = SettingsProvider.getSettings();
		store = new LogEntryStore(settings.getHost(), settings.getLogKeyspace(), settings.getLogTable(),
				settings.getUsername(), settings.getPassword(), settings.getPort());
	}

	@Test
	public void createAndRead() {

		LogEntry created = buildLogEntry("2018-01-01", "001001001", "00000001", Operation.save, "0001");

		store.create(created, 180);

		LogEntry read = store.read(created.getWriteDate(), created.hashCode());
		assertThat(read, notNullValue());
		assertThat(read, equalTo(created));
	}

	@Test
	public void createAndFind() {

		LogEntry created1 = buildLogEntry("2018-01-01", "001001001", "00000001", Operation.save, "0001");
		store.create(created1, 180);

		LogEntry created2 = buildLogEntry("2018-01-01", "001001002", "00000001", Operation.save, "0002");
		store.create(created2, 180);

		List<LogEntry> found = store.findByDate("2018-01-01");
		assertThat(found, hasSize(greaterThanOrEqualTo(2)));
		assertThat(found, hasItems(created1, created2));
	}

	private LogEntry buildLogEntry(String writeDate, String clientProductId, String partitionKey, Operation operation,
			String clusteringKey) {
		LogEntry logEntry = new LogEntry();
		logEntry.setWriteDate(writeDate);
		logEntry.setPartitionKey(partitionKey);
		logEntry.setClientProductId(clientProductId);
		logEntry.setClusteringKey(clusteringKey);
		logEntry.setOperation(operation);
		return logEntry;
	}
}