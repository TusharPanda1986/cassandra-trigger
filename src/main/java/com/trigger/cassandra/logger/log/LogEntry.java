package com.trigger.cassandra.logger.log;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class LogEntry {

	private String writeDate;
	private String keyspaceName = "ks";
	private String tableName = "ts";
	private String partitionKey;
	private Operation operation = Operation.delete;
	private String divisionIds;
	private String clusteringKey = "NA";

	@Override
	public String toString() {
		ToStringBuilder builder = new ToStringBuilder(this);
		builder.append("writeDate", getWriteDate());
		builder.append("keyspaceName", getKeyspaceName());
		builder.append("tableName", getTableName());
		builder.append("partitionKey", getPartitionKey());
		builder.append("operation", getOperation().name());
		builder.append("divisionIds", getDivisionIds());
		builder.append("clusteringKey", getClusteringKey());
		return builder.build();
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !(o instanceof LogEntry)) {
			return false;
		}

		LogEntry other = (LogEntry) o;
		return Objects.equals(this.writeDate, other.writeDate) && Objects.equals(this.keyspaceName, other.keyspaceName)
				&& Objects.equals(this.tableName, other.tableName)
				&& Objects.equals(this.partitionKey, other.partitionKey);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((writeDate == null) ? 0 : writeDate.hashCode());
		result = prime * result + ((keyspaceName == null) ? 0 : keyspaceName.hashCode());
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
		result = prime * result + ((partitionKey == null) ? 0 : partitionKey.hashCode());
		result = prime * result + ((operation == null) ? 0 : operation.hashCode());
		return result;
	}

	public String getWriteDate() {
		return writeDate;
	}

	public void setWriteDate(String writeDate) {
		this.writeDate = writeDate;
	}

	public Operation getOperation() {
		return operation;
	}

	public void setOperation(Operation operation) {
		this.operation = operation;
	}

	public String getPartitionKey() {
		return partitionKey;
	}

	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}

	public String getKeyspaceName() {
		return keyspaceName;
	}

	public void setKeyspaceName(String keyspaceName) {
		if (StringUtils.isNotBlank(keyspaceName)) {
			this.keyspaceName = keyspaceName;
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		if (StringUtils.isNotBlank(tableName)) {
			this.tableName = tableName;
		}
	}

	public String getDivisionIds() {
		return this.divisionIds;
	}

	public void setDivisionIds(String divIds) {
		this.divisionIds = divIds;
	}

	public void setClusteringKey(String clusteringKey) {
		if (StringUtils.isNotBlank(clusteringKey)) {
			this.clusteringKey = clusteringKey;
		}
	}

	public String getClusteringKey() {
		return clusteringKey;
	}
}