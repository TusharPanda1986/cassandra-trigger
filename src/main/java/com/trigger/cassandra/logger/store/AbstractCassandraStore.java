package com.trigger.cassandra.logger.store;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class AbstractCassandraStore {

	private String node;
	private Cluster cluster;
	private Session session;
	private String keyspace;
	private String table;
	private String username;
	private String password;
	private int port;

	protected AbstractCassandraStore(String node, String keyspace, String table, String username, String password,
			int port) {
		this.node = node;
		this.keyspace = keyspace;
		this.table = table;
		this.username = username;
		this.password = password;
		this.port = port;
	}

	protected Cluster getCluster() {
		if (cluster == null) {
			cluster = Cluster.builder().addContactPoint(node).withPort(port)
					.withCredentials(username, password).build();
		}
		return cluster;
	}

	protected Session getSession() {
		if (session == null) {
			session = getCluster().connect();
		}
		return session;
	}

	protected ResultSet execute(Statement statement) {
		return getSession().execute(statement);
	}

	public String getKeyspace() {
		return keyspace;
	}

	public String getTable() {
		return table;
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

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
}