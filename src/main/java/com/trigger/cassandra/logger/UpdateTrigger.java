package com.trigger.cassandra.logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Row.Deletion;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trigger.cassandra.logger.log.MapEntry;
import com.trigger.cassandra.logger.log.Operation;
import com.trigger.cassandra.logger.settings.Settings;
import com.trigger.cassandra.logger.settings.SettingsProvider;
import com.trigger.cassandra.logger.store.MapEntryStore;

public class UpdateTrigger implements ITrigger {
	private static final Logger logger = LoggerFactory.getLogger(UpdateTrigger.class);

	private MapEntryStore logEntryStore;
	private static final int TTL = 604800;

	public UpdateTrigger() {
		Settings settings = SettingsProvider.getSettings();
		this.logEntryStore = new MapEntryStore(settings.getHost(), settings.getLogKeyspace(), settings.getLogTable(),
				settings.getUsername(), settings.getPassword(), settings.getPort());
	}

	public Collection<Mutation> augment(Partition partition) {

		try {
			final String indexColumnFamily = partition.metadata().cfName;
			final String keyspaceName = partition.metadata().ksName;

			UnfilteredRowIterator unfilteredIterator = partition.unfilteredIterator();
			String partitionKey = partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
			while (unfilteredIterator.hasNext()) {
				MapEntry entry = new MapEntry();
				entry.setOperation(Operation.save);

				String clusterKeyData = "";
				Unfiltered next = unfilteredIterator.next();
				Object cluster = next.clustering();
				Map<String, Object> dataMap = new HashMap<String, Object>();
				if (cluster instanceof Clustering) {
					clusterKeyData = processUpdatedRecords(partition, partitionKey, entry, cluster, dataMap);
				}

				if (cluster instanceof ClusteringBound) {
					clusterKeyData = processDeletedRecords(partition, entry, clusterKeyData, cluster, dataMap);
				}

				setUpdateForLater(dataMap, indexColumnFamily, partitionKey, clusterKeyData, keyspaceName, entry);

			}
		} catch (RuntimeException e) {
			logger.error("exception : ", e);
		}

		return null;
	}

	private String processUpdatedRecords(Partition partition, String partitionKey, MapEntry entry, Object cluster,
			Map<String, Object> dataMap) {
		String clusterKeyData;
		Clustering clustering = (Clustering) cluster;
		clusterKeyData = clustering.toString(partition.metadata());
		Row row = partition.getRow((Clustering) clustering);
		Iterable<Cell> cells = row.cells();
		Deletion deletion = row.deletion();

		if (deletion != null && !deletion.isLive()) {
			entry.setOperation(Operation.delete);
		} else {
			processUpdates(partitionKey, entry, clusterKeyData, cells, dataMap);
		}
		return clusterKeyData;
	}

	private String extractEffectiveDate(String clusteringKey) {
		Pattern p = Pattern.compile("[^\1]*(\\d{4}-\\d{2}-\\d{2})[^\1]*");
		Matcher m = p.matcher(clusteringKey);
		if (m.matches()) {
			return m.group(1);
		}
		return clusteringKey;

	}

	private String processDeletedRecords(Partition partition, MapEntry entry, String clusterKeyData, Object cluster,
			Map<String, Object> dataMap) {
		entry.setOperation(Operation.delete);
		ClusteringBound bound = (ClusteringBound) cluster;
		List<JSONObject> bounds = new ArrayList<>();
		for (int i = 0; i < bound.size(); i++) {
			String clusteringBound = partition.metadata().comparator.subtype(i).getString(bound.get(i));
			JSONObject boundObject = new JSONObject();
			clusterKeyData = clusteringBound;
			boundObject.put("clusteringKey", clusteringBound);
			bounds.add(boundObject);
		}
		dataMap.put("deleted info", bounds);
		return clusterKeyData;
	}

	private void processUpdates(String partitionKeyData, MapEntry entry, String clusterKeyData, Iterable<Cell> cells,
			Map<String, Object> dataMap) {
		for (Cell cell : cells) {

			ColumnDefinition column = cell.column();
			String columnName = column.name + "";
			AbstractType<Object> cellValueType = (AbstractType<Object>) column.cellValueType();
			Object cellValue = cellValueType.compose(cell.value());
			AbstractType<?> columnType = column.type;

			populateLogEntries(entry, column.toString(), cell.value().array());

			if (cellValue != null) {

				if (cell.isCounterCell()) {
					entry.setOperation(Operation.save);
					dataMap.put(columnName, cellValue.toString());
				} else if (columnType instanceof MapType<?, ?>) {
					processMaps(dataMap, cell, columnName, cellValue.toString(), columnType);
				} else if (columnType instanceof SetType<?>) {
					processSets(dataMap, cell, columnName, cellValue.toString(), columnType);
				} else if (columnType instanceof ListType<?>) {
					processLists(dataMap, cell, columnName, cellValue);
				} else {
					if (cell.isLive(0)) {
						dataMap.put(columnName, cellValue.toString());
					} else {
						dataMap.put(columnName, null);
					}
				}
			}

		}
	}

	private void processLists(Map<String, Object> dataMap, Cell cell, String columnName, Object cellValue) {
		try {
			String val = "";
			if (cellValue instanceof java.nio.ByteBuffer) {
				ByteBuffer buffer = (java.nio.ByteBuffer) cellValue;
				val = fetchStringFromByteBuffer(buffer);
				val = val.trim();
			} else {
				val = cellValue.toString();
			}
			if (cell.isLive(0)) {
				if (!dataMap.containsKey(columnName)) {
					ArrayList<String> arrayList = new ArrayList<String>();
					arrayList.add(val);
					dataMap.put(columnName, arrayList);

				} else {
					ArrayList<String> arrayList = (ArrayList<String>) dataMap.get(columnName);
					if (!arrayList.contains(val)) {
						arrayList.add(val);
					}
				}
			}
		} catch (Exception e) {
			logger.error("exception : ", e);
		}
	}

	private String fetchStringFromByteBuffer(java.nio.ByteBuffer byteBuffer) {
		if (byteBuffer.hasArray()) {
			return new String(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(),
					byteBuffer.remaining());
		} else {
			final byte[] b = new byte[byteBuffer.remaining()];
			byteBuffer.duplicate().get(b);
			return new String(b);
		}
	}

	private void processSets(Map<String, Object> dataMap, Cell cell, String columnName, String cellValue,
			AbstractType<?> columnType) {
		try {
			CellPath path = cell.path();
			int size = path.size();
			for (int i = 0; i < size; i++) {
				ByteBuffer byteBuffer = path.get(i);
				AbstractType<Object> keysType = ((SetType) columnType).getElementsType();
				if (keysType.compose(byteBuffer) != null) {
					cellValue = keysType.compose(byteBuffer).toString();
				}

			}
			if (!dataMap.containsKey(columnName)) {
				ArrayList<Object> arrayList = new ArrayList<>();
				arrayList.add(cellValue);
				dataMap.put(columnName, arrayList);
			} else {
				ArrayList<Object> arrayList = (ArrayList<Object>) dataMap.get(columnName);
				if (!arrayList.contains(cellValue)) {
					arrayList.add(cellValue);
				}
			}

		} catch (Exception e) {
			logger.error("exception : ", e);
		}
	}

	private void processMaps(Map<String, Object> dataMap, Cell cell, String columnName, String cellValue,
			AbstractType<?> columnType) {

		try {
			MapType<Object, Object> mapType = (MapType<Object, Object>) columnType;

			AbstractType<Object> keysType = mapType.getKeysType();
			CellPath path = cell.path();
			int size = path.size();
			for (int i = 0; i < size; i++) {
				ByteBuffer byteBuffer = path.get(i);
				Object cellKey = keysType.compose(byteBuffer);

				if (!dataMap.containsKey(columnName)) {
					Map<Object, Object> map = new HashMap<Object, Object>();
					dataMap.put(columnName, map);

					if (cell.isLive(0)) {
						map.put(cellKey.toString(), cellValue);
					} else {
						map.put(cellKey.toString(), null);
					}
				} else {
					Map<Object, Object> map = (Map<Object, Object>) dataMap.get(columnName);

					if (cell.isLive(0)) {
						map.put(cellKey.toString(), cellValue);
					} else {
						map.put(cellKey.toString(), null);
					}
				}
			}
		} catch (Exception e) {
			logger.error("exception : ", e);
		}
	}

	private void setUpdateForLater(final Map<String, Object> hashMap, final String indexColumnFamily,
			final String tempPartitionKeyData, final String tempClusterKeyData, String keyspaceName, MapEntry entry) {

		try {
			entry.setWriteDate(getCurrentDate());
			entry.setWriteTime(new Date());
			entry.setKeyspaceName(keyspaceName);
			entry.setTableName(indexColumnFamily);
			entry.setPartitionKey(tempPartitionKeyData);
			entry.setClusteringKey(tempClusterKeyData);
			entry.setEffectiveDate(extractEffectiveDate(tempClusterKeyData));
			if (hashMap != null) {
				entry.setChangedValues(new ObjectMapper().writeValueAsString(hashMap));
			}

			logEntryStore.create(entry, TTL);
		} catch (IOException e) {
			logger.error("exception : ", e);
		}

	}

	private static String getCurrentDate() {
		return new SimpleDateFormat("yyyy-MM-dd").format(new Date());
	}

	private void populateLogEntries(MapEntry entry, String column, byte[] cellValue) {
		String value = new String(cellValue);
		logger.debug(String.format("Column being processed : %s  and the corresponding value : %s ", column, value));
		switch (column.trim().toLowerCase()) {
		case "division_ids":

			if (StringUtils.isNotBlank(value)) {
				entry.setDivisionIds(value);
			}
			break;

		default:
			break;
		}

	}
}