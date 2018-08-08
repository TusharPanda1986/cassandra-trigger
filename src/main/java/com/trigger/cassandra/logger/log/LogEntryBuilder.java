package com.trigger.cassandra.logger.log;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogEntryBuilder {

	private static final Logger logger = LoggerFactory.getLogger("LogEntryBuilder");
	
	public LogEntry build(Partition update) {
		LogEntry logEntry = new LogEntry();

		logEntry.setWriteDate(getCurrentDate());
		logEntry.setPartitionKey(update.metadata().getKeyValidator().getString(update.partitionKey().getKey()));

		try {
			
			UnfilteredRowIterator it = update.unfilteredIterator();
			while (it.hasNext()) {
				Unfiltered un = it.next();
				evaluateTypeOfOperation(un, logEntry);
				Clustering clt = (Clustering) un.clustering();
				logEntry.setClusteringKey(clt.toString(update.metadata()));
				Iterator<Cell> cells = update.getRow(clt).cells().iterator();
				Iterator<ColumnDefinition> columns = update.getRow(clt).columns().iterator();

				while (columns.hasNext()) {
					ColumnDefinition columnDef = columns.next();
					Cell cell = cells.next();
					populateLogEntries(logEntry, columnDef.toString(), cell.value().array());
				}
			}
		} catch (Exception e) {
			logger.debug(String.format("Error Occured : %s", e.getMessage()));
			logEntry.setOperation(Operation.delete);
		}

		return logEntry;
	}

	private void evaluateTypeOfOperation(Unfiltered unfiltered, LogEntry logEntry) {
		logger.debug(String.format("Type of delete : %s", unfiltered.kind().name()));
		switch (unfiltered.kind()) {
		case ROW:
			Row row = (Row) unfiltered;

			if (!row.deletion().isLive()) {
				logEntry.setOperation(Operation.delete);
			}

			for (Cell cell : row.cells()) {
				if (cell.isTombstone()) {
					logEntry.setOperation(Operation.delete);
				} else {
					logEntry.setOperation(Operation.save);
				}
			}
			break;
		case RANGE_TOMBSTONE_MARKER:
			logEntry.setOperation(Operation.delete);
			break;

		default:
			logEntry.setOperation(Operation.delete);
			break;
		}

	}

	private void populateLogEntries(LogEntry logEntry, String column, byte[] byteStream) {
		String value = new String(byteStream);
		logger.debug(
				String.format("Column being processed : %s  and the corresponding value : %s ", column, value));
		switch (column.trim().toLowerCase()) {
		case "clientproduct_id":
			logEntry.setClientProductId(value);
			break;

		default:
			break;
		}

	}

	private static String getCurrentDate() {
		return new SimpleDateFormat("yyyy-MM-dd").format(new Date());
	}
}