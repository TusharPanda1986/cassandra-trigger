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
	private static final String HASH_CODE = "hashcode";
	private static final String CLUSTERING_KEY = "clustering_key";
	private static final String WRITEDATE = "writedate";
	private static final String CLIENTPRODUCT_ID = "clientproduct_id";

	public LogEntryStore(String node, String keyspace, String table, String username, String password, int port) {
		super(node, keyspace, table, username, password, port);
	}

	public void create(LogEntry entry, int ttl) {
		Statement statement = QueryBuilder.insertInto(getKeyspace(), getTable()).value(WRITEDATE, entry.getWriteDate())
				.value(HASH_CODE, entry.hashCode()).value(CLIENTPRODUCT_ID, entry.getClientProductId())
				.value(CLUSTERING_KEY, entry.getClusteringKey()).value(PARTITION_KEY, entry.getPartitionKey())
				.value(OPERATION, entry.getOperation().toString()).using(QueryBuilder.ttl(ttl))
				.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

		execute(statement);
	}

	public List<LogEntry> findByDate(String writedate) {
		Statement statement = QueryBuilder.select().all().from(getKeyspace(), getTable())
				.where(eq(WRITEDATE, writedate));

		ResultSet result = execute(statement);
		return toEntity(result.all());
	}

	public LogEntry read(String writedate, int hashCode) {
		Statement statement = QueryBuilder.select().all().from(getKeyspace(), getTable())
				.where(eq(WRITEDATE, writedate)).and(eq(HASH_CODE, hashCode));

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
		entity.setClientProductId(row.getString(CLIENTPRODUCT_ID));
		entity.setPartitionKey(row.getString(PARTITION_KEY));
		entity.setClusteringKey(row.getString(CLUSTERING_KEY));
		entity.setOperation(EnumUtils.getEnum(Operation.class, row.getString(OPERATION)));
		return entity;
	}
}