/* Run this script on the CQL shell:
 * 
 *    {CASSANDRA_HOME}/bin/cqlsh --file create-log-schema.cql
 *
 * It will create a table "product_by_timestamps" on keyspace "logger".
 * If you want to customize these names, you also need to edit cassandra-logger.properties on {CASSANDRA_HOME}/conf.
 *
 * Please customize the keyspace replication settings to better suit your clustering needs.
 */
 
CREATE KEYSPACE IF NOT EXISTS product_v2
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE product_v2;

DROP TABLE IF EXISTS product_by_timestamps;

CREATE TABLE product_by_timestamps(
	writedate text,
	keyspace_name text,
	table_name text,
	partition_key text,
	clustering_key text,
	operation text,
	division_ids text,
	PRIMARY KEY ((writedate, keyspace_name,table_name) , partition_key)
);


CREATE TABLE product_by_maps(
	write_date text,
	keyspace_name text,
	table_name text,
	write_time timestamp,
	effective_date text,
	hash_value text,
	partition_key text,
	clustering_key text,
	operation text,
	division_ids text,
	changed_values text,
	PRIMARY KEY ((write_date, keyspace_name,table_name), hash_value)
);