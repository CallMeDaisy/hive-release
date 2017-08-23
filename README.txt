Description
===========

This is forked from hortonworks/hive-release(HDP-2.5.5.0-tag)

Release Notes
=============

* Support multi-character as delimiter when create table.


Instructions For Use
====================

* Example

  CREATE EXTERNAL TABLE y1_ex (c1 INT, c2 INT, c3 INT, c4 STRING) 
  ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.MultiCharDelimitedSerde’ 
  WITH SERDEPROPERTIES('field_delimited' =  '|++|')
  STORED AS TEXTFILE LOCATION '/hive-ex/y1';

* Description 
  
  field_delimited: multi-character used to seperate the row data.


Apache Hive (TM) 1.2.1
======================

The Apache Hive (TM) data warehouse software facilitates querying and
managing large datasets residing in distributed storage. Built on top
of Apache Hadoop (TM), it provides:

* Tools to enable easy data extract/transform/load (ETL)

* A mechanism to impose structure on a variety of data formats

* Access to files stored either directly in Apache HDFS (TM) or in other
  data storage systems such as Apache HBase (TM)

* Query execution using Apache Hadoop MapReduce, Apache Tez
  or Apache Spark frameworks.
  

Requirements
============

- Java 1.7

- Hadoop 1.x, 2.x


