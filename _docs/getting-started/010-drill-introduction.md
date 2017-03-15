---
title: "Drill Introduction"
date: 2017-03-15 02:28:07 UTC
parent: "Getting Started"
---
Drill is an Apache open-source SQL query engine for Big Data exploration.
Drill is designed from the ground up to support high-performance analysis on
the semi-structured and rapidly evolving data coming from modern Big Data
applications, while still providing the familiarity and ecosystem of ANSI SQL,
the industry-standard query language. Drill provides plug-and-play integration
with existing Apache Hive and Apache HBase deployments.  

## What's New in Apache Drill 1.10  

Drill 1.10 provides the following new features and improvements:  

* Support for the [CREATE TEMPORARY TABLE AS (CTTAS)]({{site.baseurl}}/docs/create-temporary-table-as/) command.
* A [JDBC connection option]({{site.baseurl}}/docs/using-the-jdbc-driver/#using-the-jdbc-url-format-for-a-direct-drillbit-connection) that improves fault tolerance when connecting directly to a Drill node from a client.
* The [Web Console]({{site.baseurl}}/docs/identifying-multiple-drill-versions-in-a-cluster) displays the Drill version and additional query profile statistics.
* Drill implicitly interprets the [INT96]({{site.baseurl}}/docs/parquet-format/#about-int96-support/) timestamp data type in Parquet files.
* Support for Kerberos authentication between the client and drillbit.  
  

## What's New in Apache Drill 1.9  

Drill 1.9 provides the following new features:  

* Asynchronous Parquet reader
* Parquet filter pushdown
* Dynamic UDF support
* HTTPD format plugin   

## What's New in Apache Drill 1.8  

Drill 1.8 provides the following new features and changes: 

* Metadata cache pruning
* IF EXISTS parameter with the DROP TABLE and DROP VIEW commands
* DESCRIBE SCHEMA command
* Multi-byte delimiter support
* New parameters for filter selectivity estimates  
* Changes to the configuration and launch scripts - See [Configuration and Launch Script Changes]({{site.baseurl}}/docs/apache-drill-1-8-0-release-notes/#configuration-and-launch-script-changes)

## What's New in Apache Drill 1.7  

Drill 1.7 provides the following new features:  

* Monitoring via JMX
* Hive CHAR data type support
* HBase 1.x support  

## What's New in Apache Drill 1.6  

Drill 1.6 provides the following new features:  

* Inbound impersonation 
* Additional custom window frames 

## What's New in Apache Drill 1.5  

Drill 1.5 provides the following new features:  

* Authentication and security for the Web interface and REST API
* Experimental query support for Apache Kudu (incubating)
* An improved memory allocator
* Configurable caching for Hive metadata

## What's New in Apache Drill 1.4

Drill 1.4 introduces the following improvements:

* [select with options]({{site.baseurl}}/docs/plugin-configuration-basics/#using-the-formats-attributes-as-table-function-parameters) that you use in queries to change storage plugin settings
* Improved behavior when parsing CSV file header names
* A variable to set non-pretty, such as compact, printing of JSON
* Better drillbit.log files that include query text

Drill 1.4 fixes an error that occurred when you query a Hive table using the HBaseStorageHandler ([DRILL-3739](https://issues.apache.org/jira/browse/DRILL-3739)). To successfully query a Hive table using the HBaseStorageHandler, you need to configure the Hive storage plugin as described in the [Hive storage plugin documentation]({{site.baseurl}}/docs/hive-storage-plugin/#connect-drill-to-the-hive-remote-metastore).

## What's New in Apache Drill 1.3 
This releases fix issues and add a number of enhancements, including the following ones:

* [Enhanced Amazon S3 support]({{site.baseurl}}/docs/s3-storage-plugin/)  
* Hetrogeneous types  
  Support for columns that evolve from one data type to another over time. 
* [Text file headers]({{site.baseurl}}/docs/text-files-csv-tsv-psv/#using-a-header-in-a-file)
* [Sequence files support]({{site.baseurl}}/docs/querying-sequence-files/)
* Enhancements related to querying Hive tables, MongoDB collections, and Avro files

## What's New in Apache Drill 1.2

This release of Drill fixes [many issues]({{site.baseurl}}/docs/apache-drill-1-2-0-release-notes/) and introduces a number of enhancements, including the following ones:

* Support for JDBC data sources, such as MySQL, through a [new JDBC Storage plugin](https://issues.apache.org/jira/browse/DRILL-3180)  
* Improvements in the Drill JDBC driver including inclusion of
Javadocs and better application dependency compatibility  
* Enhancements to Avro file formats  
  * [Support for complex data types](https://issues.apache.org/jira/browse/DRILL-3565), such as UNION and MAP  
  * [Optimized Avro file processing](https://issues.apache.org/jira/browse/DRILL-3720) (block-wise)  
* Partition pruning improvements
* A number of new [SQL window functions]({{site.baseurl}}/docs/sql-window-functions)  
  * NTILE  
  * LAG and LEAD  
  * FIRST_VALUE and LAST_VALUE  
* [HTTPS support]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/) for Web Console operations  
* Performance improvements for [querying HBase]({{site.baseurl}}/docs/querying-hbase/#querying-big-endian-encoded-data), which includes leveraging [ordered byte encoding]({{site.baseurl}}/docs/querying-hbase/#leveraging-hbase-ordered-byte-encoding)  
* [Optimized reads]({{site.baseurl}}/docs/querying-hive/#optimizing-reads-of-parquet-backed-tables) of Parquet-backed, Hive tables  
* Read support for the [Parquet INT96 type]({{site.baseurl}}/docs/parquet-format/#about-int96-support) and a new TIMESTAMP_IMPALA type used with the [CONVERT_FROM]({{site.baseurl}}/docs/supported-data-types/#data-types-for-convert_to-and-convert_from-functions) function decodes a timestamp from Hive or Impala.  
* [Parquet metadata caching]({{site.baseurl}}/docs/optimizing-parquet-metadata-reading/) to improve query performance on a large number of files
* DROP TABLE command  
* Improved correlated subqueries  
* Union Distinct  
* Improved LIMIT processing

## What's New in Apache Drill 1.1

Many enhancements in Apache Drill 1.1 include the following key features:

* [SQL window functions]({{site.baseurl}}/docs/sql-window-functions)
* [Partitioning data]({{site.baseurl}}) using the new [PARTITION BY]({{site.baseurl}}/docs/partition-by-clause) clause in the CTAS command
* [Delegated Hive impersonation]({{site.baseurl}}/docs/configuring-user-impersonation-with-hive-authorization/)
* Support for UNION and UNION ALL and better optimized plans that include UNION.

## What's New in Apache Drill 1.0

Apache Drill 1.0 offers the following new features:

* Many performance planning and execution [improvements](/docs/performance-tuning-introduction/).
* Updated [Drill shell]({{site.baseurl}}/docs/configuring-the-drill-shell) now formats query results.
* [Query audit logging]({{site.baseurl}}/docs/getting-query-information/) for getting the query history on a Drillbit.
* Improved connection handling.
* New Errors tab in the Query Profiles UI that facilitates troubleshooting and distributed storing of profiles.
* Support for a new storage plugin input format: [Avro](http://avro.apache.org/docs/current/spec.html)

In this release, Drill disables the DECIMAL data type, including casting to DECIMAL and reading DECIMAL types from Parquet and Hive. You can [enable the DECIMAL type](docs/supported-data-types/#enabling-the-decimal-type), but this is not recommended.

## Apache Drill Key Features

Key features of Apache Drill are:

  * Low-latency SQL queries
  * Dynamic queries on self-describing data in files (such as JSON, Parquet, text) and HBase tables, without requiring metadata definitions in the Hive metastore.
  * ANSI SQL
  * Nested data support
  * Integration with Apache Hive (queries on Hive tables and views, support for all Hive file formats and Hive UDFs)
  * BI/SQL tool integration using standard JDBC/ODBC drivers

##Quick Links
If you've never used Drill, visit these links to get a jump start:

* [Drill in 10 Minutes]({{ site.baseurl }}/docs/drill-in-10-minutes/)
* [Query Files]({{ site.baseurl }}/docs/querying-a-file-system)
* [Query HBase]({{ site.baseurl }}/docs/querying-hbase)
* [SQL Support]({{ site.baseurl }}/docs/sql-reference-introduction/)
* [Drill Tutorials]({{ site.baseurl }}/docs/tutorials-introduction)

