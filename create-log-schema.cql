/* Run this script on the CQL shell:
 * 
 *    {CASSANDRA_HOME}/bin/cqlsh --file create-log-schema.cql
 *
 * It will create a table "product_by_timestamps" on keyspace "product_v2_dev".
 * If you want to customize these names, you also need to edit cassandra-logger.properties on {CASSANDRA_HOME}/conf.
 *
 * Please customize the keyspace replication settings to better suit your clustering needs.
 */
 
CREATE KEYSPACE IF NOT EXISTS product_v2_dev
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE product_v2_dev;

DROP TABLE IF EXISTS product_by_timestamps;

CREATE TABLE product_by_timestamps(
	writedate text,
	hashcode int,
	clientproduct_id text,
	clustering_key text,
	partition_key text,
	operation text,
	PRIMARY KEY (writedate, hashcode)
);
