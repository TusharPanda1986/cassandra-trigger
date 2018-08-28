/* Run this script on the CQL shell:
 * 
 *    {CASSANDRA_HOME}/bin/cqlsh --file create-log-schema.cql
 *
 * It will create a table "product_by_maps" on keyspace "product_v2".
 * If you want to customize these names, you also need to edit cassandra-logger.properties on {CASSANDRA_HOME}/conf.
 *
 * Please customize the keyspace replication settings to better suit your clustering needs.
 */
 
CREATE KEYSPACE IF NOT EXISTS product_v2
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE product_v2;

DROP TABLE IF EXISTS product_by_maps;

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