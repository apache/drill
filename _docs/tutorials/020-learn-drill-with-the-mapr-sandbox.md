---
title: "Learn Drill with the MapR Sandbox"
parent: "Tutorials"
---
This tutorial uses the MapR Sandbox, which is a Hadoop environment pre-
configured with Apache Drill.

To complete the tutorial on the MapR Sandbox with Apache Drill, work through
the following pages in order:

  * [Installing the Apache Drill Sandbox](/docs/installing-the-apache-drill-sandbox)
  * [Getting to Know the Drill Setup](/docs/getting-to-know-the-drill-sandbox)
  * [Lesson 1: Learn About the Data Set](/docs/lession-1-learn-about-the-data-set)
  * [Lesson 2: Run Queries with ANSI SQL](/docs/lession-2-run-queries-with-ansi-sql)
  * [Lesson 3: Run Queries on Complex Data Types](/docs/lession-3-run-queries-on-complex-data-types)
  * [Summary](/docs/summary)

## About Apache Drill

Drill is an Apache open-source SQL query engine for Big Data exploration.
Drill is designed from the ground up to support high-performance analysis on
the semi-structured and rapidly evolving data coming from modern Big Data
applications, while still providing the familiarity and ecosystem of ANSI SQL,
the industry-standard query language. Drill provides plug-and-play integration
with existing Apache Hive and Apache HBase deployments.Apache Drill 0.5 offers
the following key features:

  * Low-latency SQL queries
  * Dynamic queries on self-describing data in files (such as JSON, Parquet, text) and MapR-DB/HBase tables, without requiring metadata definitions in the Hive metastore.
  * ANSI SQL
  * Nested data support
  * Integration with Apache Hive (queries on Hive tables and views, support for all Hive file formats and Hive UDFs)
  * BI/SQL tool integration using standard JDBC/ODBC drivers

## MapR Sandbox with Apache Drill

MapR includes Apache Drill as part of the Hadoop distribution. The MapR
Sandbox with Apache Drill is a fully functional single-node cluster that can
be used to get an overview on Apache Drill in a Hadoop environment. Business
and technical analysts, product managers, and developers can use the sandbox
environment to get a feel for the power and capabilities of Apache Drill by
performing various types of queries. Once you get a flavor for the technology,
refer to the [Apache Drill web site](http://drill.apache.org) and
[Apache Drill documentation
](/docs)for more
details.

Note that Hadoop is not a prerequisite for Drill and users can start ramping
up with Drill by running SQL queries directly on the local file system. Refer
to [Apache Drill in 10 minutes](/docs/apache-drill-in-10-minutes) for an introduction to using Drill in local
(embedded) mode.

