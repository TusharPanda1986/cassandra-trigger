package com.trigger.cassandra.logger;

import java.util.Collection;
import java.util.Collections;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.triggers.ITrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trigger.cassandra.logger.log.LogEntry;
import com.trigger.cassandra.logger.log.LogEntryBuilder;
import com.trigger.cassandra.logger.settings.Settings;
import com.trigger.cassandra.logger.settings.SettingsProvider;
import com.trigger.cassandra.logger.store.LogEntryStore;

public class LogTrigger implements ITrigger {
	private static final Logger logger = LoggerFactory.getLogger("LogTrigger");
	private static final int TTL = 604800;

	private LogEntryBuilder logEntryBuilder;
	private LogEntryStore logEntryStore;

	public LogTrigger() {
		Settings settings = SettingsProvider.getSettings();
		this.logEntryStore = new LogEntryStore(settings.getHost(), settings.getLogKeyspace(), settings.getLogTable(),
				settings.getUsername(), settings.getPassword(), settings.getPort());
		this.logEntryBuilder = new LogEntryBuilder();
	}

	public Collection<Mutation> augment(Partition update) {
		try {
			LogEntry logEntry = logEntryBuilder.build(update);
			logger.debug(String.format("Processing log entry: %s", logEntry));
			logEntryStore.create(logEntry, TTL);
		} catch (Exception exception) {
			try {
				logger.debug(String.format(
						"Exception while processing update from keyspace %s, table %s and columns %s: %s",
						update.metadata().ksName, update.metadata().cfName,
						update.metadata().getKeyValidator().getString(update.partitionKey().getKey()), exception));
			} catch (Exception nestedException) {
				logger.debug(String.format(
						"Can't get keyspace, table or key text from byte partition key %s and column family %s: %s",
						update.metadata().getKeyValidator().getString(update.partitionKey().getKey()), update,
						nestedException));
			}
		}

		return Collections.emptyList();
	}
}