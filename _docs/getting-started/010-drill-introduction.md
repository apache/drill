---
title: "Drill Introduction"
parent: "Getting Started"
---
Drill is an Apache open-source SQL query engine for Big Data exploration.
Drill is designed from the ground up to support high-performance analysis on
the semi-structured and rapidly evolving data coming from modern Big Data
applications, while still providing the familiarity and ecosystem of ANSI SQL,
the industry-standard query language. Drill provides plug-and-play integration
with existing Apache Hive and Apache HBase deployments. 

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

