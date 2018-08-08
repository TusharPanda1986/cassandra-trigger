package com.trigger.cassandra.logger.settings;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class Settings {

	private String logKeyspace;
	private String logTable;
	private int port;
	private String username;
	private String password;
	private String host;

	public String getLogKeyspace() {
		return logKeyspace;
	}

	public void setLogKeyspace(String logKeyspace) {
		this.logKeyspace = logKeyspace;
	}

	public String getLogTable() {
		return logTable;
	}

	public void setLogTable(String logTable) {
		this.logTable = logTable;
	}

	@Override
	public String toString() {
		ToStringBuilder builder = new ToStringBuilder(this);
		builder.append("logKeyspace", getLogKeyspace());
		builder.append("logTable", getLogTable());
		builder.append("port", getPort());
		builder.append("username", getUsername());
		builder.append("password", getPassword());
		builder.append("host", getHost());
		return builder.build();
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}
}