package com.trigger.cassandra.logger.log;

import java.util.Date;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class MapEntry {

	private String writeDate;
	private Date writeTime;
	private String keyspaceName = "ks";
	private String tableName = "ts";
	private String partitionKey;
	private Operation operation = Operation.delete;
	private String divisionIds;
	private String clusteringKey = "NA";
	private String changedValues;
	private String effectiveDate = "NA";

	@Override
	public String toString() {
		ToStringBuilder builder = new ToStringBuilder(this);
		builder.append("writeDate", getWriteDate());
		builder.append("writeTime", getWriteTime());
		builder.append("keyspaceName", getKeyspaceName());
		builder.append("tableName", getTableName());
		builder.append("partitionKey", getPartitionKey());
		builder.append("operation", getOperation().name());
		builder.append("divisionIds", getDivisionIds());
		builder.append("clusteringKey", getClusteringKey());
		builder.append("changedValues", getChangedValues());
		builder.append("effectiveDate", getEffectiveDate());
		return builder.build();
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !(o instanceof MapEntry)) {
			return false;
		}

		MapEntry other = (MapEntry) o;
		return Objects.equals(this.writeDate, other.writeDate) && Objects.equals(this.keyspaceName, other.keyspaceName)
				&& Objects.equals(this.tableName, other.tableName) && Objects.equals(this.writeTime, other.writeTime)
				&& Objects.equals(this.partitionKey.hashCode(), other.partitionKey.hashCode())
				&& Objects.equals(this.effectiveDate, other.effectiveDate);
	}

	public String getHashCode() {
		return this.partitionKey.hashCode() + "" + this.effectiveDate.hashCode();
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

	public Date getWriteTime() {
		return writeTime;
	}

	public void setWriteTime(Date writeTime) {
		this.writeTime = writeTime;
	}

	public String getChangedValues() {
		return changedValues;
	}

	public void setChangedValues(String changedValues) {
		this.changedValues = changedValues;
	}

	public String getEffectiveDate() {
		return effectiveDate;
	}

	public void setEffectiveDate(String effectiveDate) {
		if (StringUtils.isNotBlank(effectiveDate)) {
			this.effectiveDate = effectiveDate;
		}
	}
}