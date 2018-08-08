CASSANDRA LOGGER
================
[![Build Status](https://api.travis-ci.org/TusharPanda1986/cassandra-trigger.svg)](https://travis-ci.org/TusharPanda1986/cassandra-trigger)

[Trigger](http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-0-prototype-triggers-support) for [Apache Cassandra](http://cassandra.apache.org) that keeps a log of all updates in a set of tables. Useful to sync Cassandra with other databases, such as Solr, Elasticsearch or even traditional RDBMS.

REQUIREMENTS
------------

- [Cassandra](http://wiki.apache.org/cassandra/GettingStarted) 3.0+
- [Oracle JDK](http://docs.oracle.com/javase/7/docs/webnotes/install) 8
- [Gradle](http://gradle.org/installation) 2.2

USAGE
-----

For each table you want to log, you need to create a trigger using the following CQL statement:

    CREATE TRIGGER <trigger_name> ON <table> USING 'com.trigger.cassandra.logger.LogTrigger';

For instance:

    CREATE TRIGGER logger ON product USING 'com.trigger.cassandra.logger.LogTrigger';

If you want to disable this trigger, you can use:

    DROP TRIGGER logger ON product;

Every `INSERT`, `UPDATE` or `DELETE` made on a table that has a log trigger enabled will be logged on table `product_by_timestamps`.

You can customize the name and keyspace of the log table by editing file [`cassandra-logger.properties`](#customization).

ASSUMPTIONS ABOUT YOUR SCHEMA
-----------------------------

The logger supports tables with composite primary keys.



SETUP
-----

### Installing Cassandra

The trigger API was released as part of Cassandra 3.0. This trigger *will not work* with versions of Cassandra previous to 2.1.

Please follow the instructions from the Cassandra project [website](http://wiki.apache.org/cassandra/GettingStarted).

### Automatic Log Trigger Installation

The script [`install-cassandra-trigger.sh`](install-cassandra-trigger.sh) will build and install the trigger on Cassandra automatically:

    ./install-cassandra-trigger {CASSANDRA_HOME}

where `{CASSANDRA_HOME}` is the root of your Cassandra installation. This directory needs to be writable by the user.

*Please notice that the trigger needs to be installed on every node of your cluster.*

### Manual Log Trigger Installation

In case you are deploying to a multi-node clustered environment or need to troubleshoot the installation, you can install the trigger manually.

1. Download the jar from one of the [releases](https://github.com/TusharPanda1986/cassandra-trigger/releases).

2. Copy the jar to `{CASSANDRA_HOME}/conf/triggers`

3. Start Cassandra. If it is already running, you can force reloading of the triggers by using:

        {CASSANDRA_HOME}/bin/nodetool -h localhost reloadtriggers

  You should see a line like this at `{CASSANDRA_HOME}/logs/system.log`:

        INFO  [...] 2018-02-26 12:51:09,933 CustomClassLoader.java:87 - Loading new jar /.../apache-cassandra/conf/triggers/cassandra-trigger.jar

### Create the Log Schema

*Before using the trigger you MUST create the log table schema.*

To do this, load script [`create-log-schema.cql`](create-log-schema.cql) into CQL shell:
 
    {CASSANDRA_HOME}/bin/cqlsh --file create-log-schema.sql

To make sure it was created correctly, enter CQL shell and run:

    DESCRIBE TABLE product_v2_dev.product_by_timestamps

By default, the logger will use table `product_by_timestamps` and keyspace `product_v2_dev`. You can customize this by editing `cassandra-logger.properties`.

CUSTOMIZATION
-------------

In order to customize the names of the keyspace and table used by the logger, copy the file [`cassandra-logger.properties`](config/cassandra-logger.properties) to `{CASSANDRA_HOME}/conf` and edit it to better suit your needs. The installation script will copy this file for you automatically.

*If you change the default keyspace or table names, you need to recreate the log schema with those names.*

EXAMPLES
--------

For illustration purposes, let's create an example schema:

    CREATE KEYSPACE example WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    USE example;

Suppose we have the following example table:
        
    CREATE TABLE sample_table (
	A text,B text,C timestamp,D text,E text,F text,G text,H boolean,I text,
	clientproduct_id text,
	PRIMARY KEY (( A, B ), C, D, E, F, G, H,
	I)) WITH CLUSTERING ORDER BY ( C DESC, D ASC, E ASC, F ASC, G ASC, H ASC, I ASC );

Let's add a log trigger to it:

    CREATE TRIGGER logger ON sample_table USING 'com.trigger.cassandra.logger.LogTrigger';
 
We then create some products:
 
    INSERT INTO sample_table (A,B,C,D,E,F,G,H,I,clientproduct_id) VALUES     		('7','11',1496300400000,'2734','1000','2002','00000020',false,'child','99');
    INSERT INTO sample_table (A,B,C,D,E,F,G,H,I,clientproduct_id) VALUES 		('8','11',1496300400000,'2734','1000','2003','00000020',false,'child','100');
    INSERT INTO sample_table (A,B,C,D,E,F,G,H,I,clientproduct_id) VALUES 		('9','11',1496300400000,'2734','1000','2004','00000020',false,'child','101');
    INSERT INTO sample_table (A,B,C,D,E,F,G,H,I,clientproduct_id) VALUES 		('10','11',1496300400000,'2734','1000','2005','00000020',false,'child','102');
        
Which gives us:
 
 a  | b  | c                               | d    | e    | f    | g        | h     | i     | clientproduct_id
----+----+---------------------------------+------+------+------+----------+-------+-------+------------------
  7 | 11 | 2017-06-01 07:00:00.000000+0000 | 2734 | 1000 | 2002 | 00000020 | False | child |               99
  9 | 11 | 2017-06-01 07:00:00.000000+0000 | 2734 | 1000 | 2004 | 00000020 | False | child |              101
 10 | 11 | 2017-06-01 07:00:00.000000+0000 | 2734 | 1000 | 2005 | 00000020 | False | child |              102
  8 | 11 | 2017-06-01 07:00:00.000000+0000 | 2734 | 1000 | 2003 | 00000020 | False | child |              100


Now, querying the log table we can see that there's an entry for each product we created:

     writedate  | hashcode   | clientproduct_id | clustering_key| operation | partition_key
     2018-08-06 | 1506703837 |               99 | c=2017-06-01 00:00-0700, d=2734, e=1000, f=2002, g=00000020, h=false, i=child |      save |          7:11
     2018-08-06 | 1548666865 |              101 | c=2017-06-01 00:00-0700, d=2734, e=1000, f=2004, g=00000020, h=false, i=child |      save |          9:11
     2018-08-06 | 1565885122 |              102 | c=2017-06-01 00:00-0700, d=2734, e=1000, f=2005, g=00000020, h=false, i=child |      save |         10:11
    2018-08-06 | 1842852544 |              100 | c=2017-06-01 00:00-0700, d=2734, e=1000, f=2003, g=00000020, h=false, i=child |      save |          8:11

     
Let's delete one product:

    DELETE FROM sample_table WHERE a='7' and b='11';
    
The log table now contains a delete entry:

     writedate  | hashcode   | clientproduct_id | clustering_key| operation | partition_key
     2018-08-06 | 1506703837 |               99 | c=2017-06-01 00:00-0700, d=2734, e=1000, f=2002, g=00000020, h=false, i=child |      save |          7:11
     2018-08-06 | 1548666865 |              101 | c=2017-06-01 00:00-0700, d=2734, e=1000, f=2004, g=00000020, h=false, i=child |      save |          9:11
     2018-08-06 | 1565885122 |              102 | c=2017-06-01 00:00-0700, d=2734, e=1000, f=2005, g=00000020, h=false, i=child |      save |         10:11
    2018-08-06 | 1842852544 |              100 | c=2017-06-01 00:00-0700, d=2734, e=1000, f=2003, g=00000020, h=false, i=child |      save |          8:11
    2018-08-06 | -1042852544 |                 |  |      delete |          7:11
             

AUTOMATED TESTS
---------------

### Running Unit Tests (Java)

    gradle test

You need to have Cassandra running and the product_v2_dev schema created, otherwise tests will fail.

The integration test is disabled so inorder to execute the integration test, search for a file named :

    LogEntryStoreIntegrationTest.java
    
and execute it staandalone or else remove the following lines from build.gradle:

    test {
    exclude '**/LogEntryStoreIntegrationTest.class'
    }