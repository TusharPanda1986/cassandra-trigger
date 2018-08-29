package com.trigger.cassandra.logger.store;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.trigger.cassandra.logger.log.MapEntry;
import com.trigger.cassandra.logger.log.Operation;
import com.trigger.cassandra.logger.settings.Settings;
import com.trigger.cassandra.logger.settings.SettingsProvider;

public class MapLogEntryStoreIntegrationTest {
	private static final String KEYSPACE = "ks";
	private static final String TABLE = "ts";
	MapEntryStore store;

	@Before
	public void setup() {
		Settings settings = SettingsProvider.getSettings();
		store = new MapEntryStore(settings.getHost(), settings.getLogKeyspace(), settings.getLogTable(),
				settings.getUsername(), settings.getPassword(), settings.getPort());
	}

	@Test
	public void createAndFind() throws InterruptedException {

		DateTime startDate = new DateTime();
		startDate.minusMinutes(5);

		DateTime endDate = new DateTime();
		endDate.plusMinutes(5);

		MapEntry created1 = buildLogEntry("2018-01-01", new DateTime().toDate(), "3pk", "3ck", Operation.save, KEYSPACE,
				TABLE, "divs", "changed", "2018-01-01");
		System.out.println(created1.toString());
		store.create(created1, 180);

		Thread.sleep(10);

		MapEntry created2 = buildLogEntry("2018-01-01", new Date(), "3pk", "3ck", Operation.save, KEYSPACE, TABLE,
				"divs", "changed", "2018-01-02");
		System.out.println(created2.toString());
		store.create(created2, 180);

		List<MapEntry> found = store.findByLogPartition("2018-01-01", KEYSPACE, TABLE);
		assertThat(found, hasSize(greaterThanOrEqualTo(2)));
		assertThat(found, hasItems(created1, created2));
	}

	private MapEntry buildLogEntry(String writeDate, Date writeTime, String partitionKey, String clusteringKey,
			Operation operation, String keyspace, String table, String divIds, String changedValues,
			String effectiveDate) {
		MapEntry logEntry = new MapEntry();
		logEntry.setWriteDate(writeDate);
		logEntry.setKeyspaceName(keyspace);
		logEntry.setTableName(table);
		logEntry.setWriteTime(writeTime);
		logEntry.setPartitionKey(partitionKey);
		logEntry.setClusteringKey(clusteringKey);
		logEntry.setOperation(operation);
		logEntry.setDivisionIds(divIds);
		logEntry.setChangedValues(changedValues);
		logEntry.setEffectiveDate(effectiveDate);
		return logEntry;
	}
}