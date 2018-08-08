package com.trigger.cassandra.logger.log;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class LogEntry {

	private String writeDate;
	private String clientProductId;
	private String partitionKey;
	private String clusteringKey;
	private Operation operation = Operation.delete;

	@Override
	public String toString() {
		ToStringBuilder builder = new ToStringBuilder(this);
		builder.append("writeDate", getWriteDate());
		builder.append("clientProductId", getClientProductId());
		builder.append("partitionKey", getPartitionKey());
		builder.append("clusteringKey", getClusteringKey());
		builder.append("operation", getOperation().name());
		return builder.build();
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !(o instanceof LogEntry)) {
			return false;
		}

		LogEntry other = (LogEntry) o;
		return Objects.equals(this.writeDate, other.writeDate) && Objects.equals(this.hashCode(), other.hashCode());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((writeDate == null) ? 0 : writeDate.hashCode());
		result = prime * result + ((partitionKey == null) ? 0 : partitionKey.hashCode());
		result = prime * result + ((clusteringKey == null) ? 0 : clusteringKey.hashCode());
		result = prime * result + ((operation == null) ? 0 : operation.hashCode());
		return result;
	}

	public String getWriteDate() {
		return writeDate;
	}

	public void setWriteDate(String writeDate) {
		this.writeDate = writeDate;
	}

	public String getClientProductId() {
		return clientProductId;
	}

	public void setClientProductId(String clientProductId) {
		this.clientProductId = clientProductId;
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

	public String getClusteringKey() {
		return clusteringKey;
	}

	public void setClusteringKey(String clusteringKey) {
		this.clusteringKey = clusteringKey;
	}
}