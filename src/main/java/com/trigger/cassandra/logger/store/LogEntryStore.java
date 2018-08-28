package com.trigger.cassandra.logger.store;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.List;

import org.apache.commons.lang3.EnumUtils;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import com.trigger.cassandra.logger.log.LogEntry;
import com.trigger.cassandra.logger.log.Operation;

public class LogEntryStore extends AbstractCassandraStore {

	private static final String OPERATION = "operation";
	private static final String PARTITION_KEY = "partition_key";
	private static final String CLUSTERING_KEY = "clustering_key";
	private static final String DIVISION_IDS = "division_ids";
	private static final String WRITEDATE = "writedate";
	private static final String KEYSPACE = "keyspace_name";
	private static final String TABLE = "table_name";

	public LogEntryStore(String node, String keyspace, String table, String username, String password, int port) {
		super(node, keyspace, table, username, password, port);
	}

	public void create(LogEntry entry, int ttl) {
		Statement statement = QueryBuilder.insertInto(getKeyspace(), getTable()).value(WRITEDATE, entry.getWriteDate())
				.value(KEYSPACE, entry.getKeyspaceName()).value(TABLE, entry.getTableName())
				.value(DIVISION_IDS, entry.getDivisionIds()).value(PARTITION_KEY, entry.getPartitionKey())
				.value(CLUSTERING_KEY, entry.getClusteringKey()).value(OPERATION, entry.getOperation().toString())
				.using(QueryBuilder.ttl(ttl)).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

		execute(statement);
	}

	public List<LogEntry> findByLogPartition(String writedate, String keyspaceName, String tableName) {
		Statement statement = QueryBuilder.select().all().from(getKeyspace(), getTable())
				.where(eq(WRITEDATE, writedate)).and(eq(KEYSPACE, keyspaceName)).and(eq(TABLE, tableName));

		ResultSet result = execute(statement);
		return toEntity(result.all());
	}

	public LogEntry read(String writedate, String keyspaceName, String tableName, String partitionKey) {
		Statement statement = QueryBuilder.select().all().from(getKeyspace(), getTable())
				.where(eq(WRITEDATE, writedate)).and(eq(KEYSPACE, keyspaceName)).and(eq(TABLE, tableName))
				.and(eq(PARTITION_KEY, partitionKey));

		ResultSet result = execute(statement);
		return toEntity(result.one());
	}

	private List<LogEntry> toEntity(List<Row> rows) {
		List<LogEntry> entities = Lists.newArrayListWithCapacity(rows.size());
		for (Row row : rows) {
			entities.add(toEntity(row));
		}
		return entities;
	}

	private LogEntry toEntity(Row row) {
		LogEntry entity = new LogEntry();
		entity.setWriteDate(row.getString(WRITEDATE));
		entity.setKeyspaceName(row.getString(KEYSPACE));
		entity.setDivisionIds(row.getString(DIVISION_IDS));
		entity.setTableName(row.getString(TABLE));
		entity.setPartitionKey(row.getString(PARTITION_KEY));
		entity.setClusteringKey(row.getString(CLUSTERING_KEY));
		entity.setOperation(EnumUtils.getEnum(Operation.class, row.getString(OPERATION)));
		return entity;
	}
}