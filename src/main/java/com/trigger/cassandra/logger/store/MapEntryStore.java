package com.trigger.cassandra.logger.store;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.EnumUtils;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import com.trigger.cassandra.logger.log.MapEntry;
import com.trigger.cassandra.logger.log.Operation;

public class MapEntryStore extends AbstractCassandraStore {

	private static final String OPERATION = "operation";
	private static final String PARTITION_KEY = "partition_key";
	private static final String CLUSTERING_KEY = "clustering_key";
	private static final String DIVISION_IDS = "division_ids";
	private static final String WRITEDATE = "write_date";
	private static final String WRITETIME = "write_time";
	private static final String KEYSPACE = "keyspace_name";
	private static final String TABLE = "table_name";
	private static final String HASH_VALUE = "hash_value";
	private static final String CHANGED_VALUES = "changed_values";
	private static final String EFF_DATE = "effective_date";

	public MapEntryStore(String node, String keyspace, String table, String username, String password, int port) {
		super(node, keyspace, table, username, password, port);
	}

	public void create(MapEntry entry, int ttl) {
		Statement statement = QueryBuilder.insertInto(getKeyspace(), getTable()).value(WRITEDATE, entry.getWriteDate())
				.value(WRITETIME, entry.getWriteTime()).value(HASH_VALUE, entry.getHashCode())
				.value(KEYSPACE, entry.getKeyspaceName()).value(TABLE, entry.getTableName())
				.value(DIVISION_IDS, entry.getDivisionIds()).value(PARTITION_KEY, entry.getPartitionKey())
				.value(CLUSTERING_KEY, entry.getClusteringKey()).value(OPERATION, entry.getOperation().toString())
				.value(CHANGED_VALUES, entry.getChangedValues()).value(EFF_DATE, entry.getEffectiveDate())
				.using(QueryBuilder.ttl(ttl)).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

		execute(statement);
	}

	public void update(MapEntry entry, int ttl) {
		Statement statement = QueryBuilder.update(getKeyspace(), getTable()).using(QueryBuilder.ttl(ttl))
				.with(QueryBuilder.set(OPERATION, entry.getOperation().toString()))
				.and(QueryBuilder.set(WRITETIME, entry.getWriteTime()))
				.where(QueryBuilder.eq(WRITEDATE, entry.getWriteDate()))
				.and(QueryBuilder.eq(KEYSPACE, entry.getKeyspaceName()))
				.and(QueryBuilder.eq(TABLE, entry.getTableName())).and(QueryBuilder.eq(HASH_VALUE, entry.getHashCode()))
				.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

		execute(statement);
	}

	public List<MapEntry> findByLogPartition(String writedate, String keyspaceName, String tableName) {
		Statement statement = QueryBuilder.select().all().from(getKeyspace(), getTable())
				.where(eq(WRITEDATE, writedate)).and(eq(KEYSPACE, keyspaceName)).and(eq(TABLE, tableName));

		ResultSet result = execute(statement);
		return toEntity(result.all());
	}

	public MapEntry read(String writedate, String keyspaceName, String tableName, Date startTime, Date endTime) {
		Statement statement = QueryBuilder.select().all().from(getKeyspace(), getTable())
				.where(eq(WRITEDATE, writedate)).and(eq(KEYSPACE, keyspaceName)).and(eq(TABLE, tableName))
				.and(gte(WRITETIME, startTime)).and(lte(WRITETIME, endTime));

		ResultSet result = execute(statement);
		return toEntity(result.one());
	}

	private List<MapEntry> toEntity(List<Row> rows) {
		List<MapEntry> entities = Lists.newArrayListWithCapacity(rows.size());
		for (Row row : rows) {
			entities.add(toEntity(row));
		}
		return entities;
	}

	private MapEntry toEntity(Row row) {
		MapEntry entity = new MapEntry();
		entity.setWriteDate(row.getString(WRITEDATE));
		entity.setWriteTime(row.getTimestamp(WRITETIME));
		entity.setKeyspaceName(row.getString(KEYSPACE));
		entity.setDivisionIds(row.getString(DIVISION_IDS));
		entity.setTableName(row.getString(TABLE));
		entity.setPartitionKey(row.getString(PARTITION_KEY));
		entity.setClusteringKey(row.getString(CLUSTERING_KEY));
		entity.setOperation(EnumUtils.getEnum(Operation.class, row.getString(OPERATION)));
		entity.setChangedValues(row.getString(CHANGED_VALUES));
		entity.setEffectiveDate(row.getString(EFF_DATE));
		return entity;
	}
}