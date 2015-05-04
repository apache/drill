---
layout: page
title: Frequently Asked Questions
---
## Overview

### Why Drill?

The 40-year monopoly of the RDBMS is over. With the exponential growth of data in recent years, and the shift towards rapid application development, new data is increasingly being stored in non-relational datastores including Hadoop, NoSQL and cloud storage. Apache Drill enables analysts, business users, data scientists and developers to explore and analyze this data without sacrificing the flexibility and agility offered by these datastores. Drill processes the data in-situ without requiring users to define schemas or transform data.

### What are some of Drill's key features?

Drill is an innovative distributed SQL engine designed to enable data exploration and analytics on non-relational datastores. Users can query the data using standard SQL and BI tools without having to create and manage schemas. Some of the key features are:

* Schema-free JSON document model similar to MongoDB and Elasticsearch
* Industry-standard APIs: ANSI SQL, ODBC/JDBC, RESTful APIs
* Extremely user and developer friendly
* Pluggable architecture enables connectivity to multiple datastores

### How does Drill achieve performance?

Drill is built from the ground up to achieve high throughput and low latency. The following capabilities help accomplish that:

* **Distributed query optimization and execution**: Drill is designed to scale from a single node (your laptop) to large clusters with thousands of servers.
* **Columnar execution**: Drill is the world's only columnar execution engine that supports complex data and schema-free data. It uses a shredded, in-memory, columnar data representation.
* **Runtime compilation and code generation**: Drill is the world's only query engine that compiles and re-compiles queries at runtime. This allows Drill to achieve high performance without knowing the structure of the data in advance. Drill leverages multiple compilers as well as ASM-based bytecode rewriting to optimize the code.
* **Vectorization**: Drill takes advantage of the latest SIMD instructions available in modern processors. 
* **Optimistic/pipelined execution**: Drill is able to stream data in memory between operators. Drill minimizes the use of disks unless needed to complete the query.

### What datastores does Drill support?

Drill is primarily focused on non-relational datastores, including Hadoop, NoSQL and cloud storage. The following datastores are currently supported:

* **Hadoop**: All Hadoop distributions (HDFS API 2.3+), including Apache Hadoop, MapR, CDH and Amazon EMR
* **NoSQL**: MongoDB, HBase
* **Cloud storage**: Amazon S3, Google Cloud Storage, Azure Blog Storage, Swift

A new datastore can be added by developing a storage plugin. Drill's unique schema-free JSON data model enables it to query non-relational datastores in-situ (many of these systems store complex or schema-free data).

### What clients are supported?

* **BI tools** via the ODBC and JDBC drivers (eg, Tableau, Excel, MicroStrategy, Spotfire, QlikView, Business Objects)
* **Custom applications** via the REST API
* **Java and C applications** via the dedicated Java and C libraries

## Comparisons

### Is  Drill a 'SQL-on-Hadoop' engine?

Drill supports a variety of non-relational datastores in addition to Hadoop. Drill takes a different approach compared to traditional SQL-on-Hadoop technologies like Hive and Impala. For example, users can directly query self-describing data (eg, JSON, Parquet) without having to create and manage schemas.

The following table provides a more detailed comparison between Drill and traditional SQL-on-Hadoop technologies:

| &nbsp; | Drill | SQL-on-Hadoop (Hive, Impala, etc.) |
|---|-------|------------------------------------|
| Use case | Self-service, in-situ, SQL-based analytics | Data warehouse offload |
| Data sources | Hadoop, NoSQL, cloud storage (including multiple instances)| A single Hadoop cluster |
| Data model | Schema-free JSON (like MongoDB) | Relational |
| User experience | Point-and-query | Ingest data &#8594; define schemas &#8594; query |
| Deployment model | Standalone service or co-located with Hadoop or NoSQL | Co-located with Hadoop |
| Data management | Self-service | IT-driven |
| SQL | ANSI SQL | SQL-like |
| 1.0 availability | Q2 2015 | Q2 2013 or earlier |

### Is Spark SQL similar to Drill?

No. Spark SQL is primarily designed to enable developers to incorporate SQL statements in Spark programs. Drill does not depend on Spark, and is targeted at business users, analysts, data scientists and developers. 

### Does Drill replace Hive?

Hive is a batch processing framework most suitable for long-running jobs. For data exploration and BI, Drill provides a much better experience than Hive.

In addition, Drill is not limited to Hadoop. For example, it can query NoSQL databases (eg, MongoDB, HBase) and cloud storage (eg, Amazon S3, Google Cloud Storage, Azure Blob Storage, Swift).

## Metadata

### How does Drill support queries on self-describing data?

Drill's flexible JSON data model and on-the-fly schema discovery enable it to query self-describing data.

* **JSON data model**: Traditional query engines have a relational data model, which is limited to flat records with a fixed structure. Drill is built from the ground up to support modern complex/semi-structured data commonly seen in non-relational datastores such as Hadoop, NoSQL and cloud storage. Drill's internal in-memory data representation is hierarchical and columnar, allowing it to perform efficient SQL processing on complex data without flattening into rows.
* **On-the-fly schema discovery (or late binding)**: Traditional query engines (eg, relational databases, Hive, Impala, Spark SQL) need to know the structure of the data before query execution. Drill, on the other hand, features a fundamentally different architecture, which enables execution to begin without knowing the structure of the data. The query is automatically compiled and re-compiled during the execution phase, based on the actual data flowing through the system. As a result, Drill can handle data with evolving schema or even no schema at all (eg, JSON files, MongoDB collections, HBase tables).

### But I already have schemas defined in Hive Metastore? Can I use that with Drill?

Absolutely. Drill has a storage plugin for Hive tables, so you can simply point Drill to the Hive Metastore and start performing low-latency queries on Hive tables. In fact, a single Drill cluster can query data from multiple Hive Metastores, and even perform joins across these datasets.

### Is Drill "anti-schema" or "anti-DBA"?

Not at all. Drill actually takes advantage of schemas when available. For example, Drill leverages the schema information in Hive when querying Hive tables. However, when querying schema-free datastores like MongoDB, or raw files on S3 or Hadoop, schemas are not available, and Drill is still able to query that data.

Centralized schemas work well if the data structure is static, and the value of data is well understood and ready to be operationalized for regular reporting purposes. However, during data exploration, discovery and interactive analysis, requiring rigid modeling poses significant challenges. For example:

* Complex data (eg, JSON) is hard to map to relational tables
* Centralized schemas are hard to keep in sync when the data structure is changing rapidly
* Non-repetitive/ad-hoc queries and data exploration needs may not justify modeling costs

Drill is all about flexibility. The flexible schema management capabilities in Drill allow users to explore raw data and then create models/structure with `CREATE TABLE` or `CREATE VIEW` statements, or with Hive Metastore.

### What does a Drill query look like?

Drill uses a decentralized metadata model and relies on its storage plugins to provide metadata. There is a storage plugin associated with each data source that is supported by Drill.

The name of the table in a query tells Drill where to get the data:

```sql
SELECT * FROM dfs1.root.`/my/log/files/`;
SELECT * FROM dfs2.root.`/home/john/log.json`;
SELECT * FROM mongodb1.website.users;
SELECT * FROM hive1.logs.frontend;
SELECT * FROM hbase1.events.clicks;
```

### What SQL functionality does Drill support?

Drill supports standard SQL (aka ANSI SQL). In addition, it features several extensions that help with complex data, such as the `KVGEN` and `FLATTEN` functions. For more details, refer to the [SQL Reference]({{ site.baseurl }}/docs/sql-reference/).

### Do I need to load data into Drill to start querying it?

No. Drill can query data 'in-situ'.

## Getting Started

### What is the best way to get started with Drill?

The best way to get started is to try it out. It only takes a few minutes and all you need is a laptop (Mac, Windows or Linux). We've compiled [several tutorials]({{ site.baseurl }}/docs/tutorials-introduction/) to help you get started.

### How can I ask questions and provide feedback?

Please post your questions and feedback to <user@drill.apache.org>. We are happy to help!

### How can I contribute to Drill?

The documentation has information on [how to contribute]({{ site.baseurl }}/docs/contribute-to-drill/).
