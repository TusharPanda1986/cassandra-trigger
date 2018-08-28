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

public class LogEntryStoreIntegrationTest {

	private static final String TABLE = "ts";
	private static final String KEYSPACE = "ks";
	LogEntryStore store;

	@Before
	public void setup() {
		Settings settings = SettingsProvider.getSettings();
		store = new LogEntryStore(settings.getHost(), settings.getLogKeyspace(), settings.getLogTable(),
				settings.getUsername(), settings.getPassword(), settings.getPort());
	}

	@Test
	public void createAndRead() {

		LogEntry created = buildLogEntry("2018-01-01", "00000001", Operation.save, KEYSPACE, TABLE);

		store.create(created, 180);

		LogEntry read = store.read(created.getWriteDate(), KEYSPACE, TABLE, created.getPartitionKey());
		assertThat(read, notNullValue());
		assertThat(read, equalTo(created));
	}

	@Test
	public void createAndFind() {

		LogEntry created1 = buildLogEntry("2018-01-01", "00000001", Operation.save, KEYSPACE, TABLE);
		System.out.println(created1.toString());
		store.create(created1, 180);

		LogEntry created2 = buildLogEntry("2018-01-01", "00000001", Operation.save, KEYSPACE, TABLE);
		System.out.println(created2.toString());
		store.create(created2, 180);

		List<LogEntry> found = store.findByLogPartition("2018-01-01", KEYSPACE, TABLE);
		assertThat(found, hasSize(greaterThanOrEqualTo(2)));
		assertThat(found, hasItems(created1, created2));
	}

	@Test
	public void testCreateDeleteRecords() {

		LogEntry created1 = buildLogEntry("2018-01-01", "00000003", Operation.save, KEYSPACE, TABLE);
		System.out.println(created1.toString());
		store.create(created1, 180);

		LogEntry created2 = buildLogEntry("2018-01-01", "00000003", Operation.delete, KEYSPACE, TABLE);
		System.out.println(created2.toString());
		store.create(created2, 180);

		List<LogEntry> found = store.findByLogPartition("2018-01-01", KEYSPACE, TABLE);
		assertThat(found, hasSize(greaterThanOrEqualTo(1)));
		assertThat(found, hasItems(created1, created2));
	}

	private LogEntry buildLogEntry(String writeDate, String partitionKey, Operation operation, String keyspace,
			String table) {
		LogEntry logEntry = new LogEntry();
		logEntry.setWriteDate(writeDate);
		logEntry.setKeyspaceName(keyspace);
		logEntry.setTableName(table);
		logEntry.setPartitionKey(partitionKey);
		logEntry.setOperation(operation);
		return logEntry;
	}
}