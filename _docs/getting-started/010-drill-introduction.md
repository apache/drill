---
title: "Drill Introduction"
date:  
parent: "Getting Started"
---
Drill is an Apache open-source SQL query engine for Big Data exploration.
Drill is designed from the ground up to support high-performance analysis on
the semi-structured and rapidly evolving data coming from modern Big Data
applications, while still providing the familiarity and ecosystem of ANSI SQL,
the industry-standard query language. Drill provides plug-and-play integration
with existing Apache Hive and Apache HBase deployments. 

## What's New in Apache Drill 1.3 and 1.4
These releases fix issues and add a number of enhancements, including the following ones:

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

