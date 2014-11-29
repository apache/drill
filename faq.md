---
layout: page
title: Frequently Asked Questions
---
## What use cases should I consider using Drill for?

Drill provides low latency SQL queries on large-scale datasets. Example use cases for Drill include

* Interactive data exploration/data discovery
* Adhoc BI/reporting queries
* Analytics on NoSQL data
* Real time or Day zero analytics (i.e analyze data as it comes with no preparation/ETL)

We expect Drill to be used in lot more use cases where low latency is required.

## Does Drill replace Hive for batch processing? What about my OLTP applications?

Drill complements batch-processing frameworks such as Hive, Pig, MapReduce to support low latency queries. Drill at this point doesn't make an optimal choice for OLTP/operational applications that require sub-second response times.
            
## There are lots of SQL on Hadoop technologies out there. How is Drill different?

Drill takes a different approach to SQL-on-Hadoop than Hive and other related technologies. The goal for Drill is to bring the SQL ecosystem and performance of the relational systems to Hadoop-scale data without compromising on the flexibility of Hadoop/NoSQL systems. Drill provides a flexible query environment for users with the key capabilities as below.

* Self-describing data support without centralized schema definitions/management
* Support for complex/multi-structured data types
* ANSI SQL support (not SQL "Like") & BI tool integration
* Extensibility to go beyond Hadoop environments

## What is self-describing data?

Self-describing data is where schema is specified as part of the data itself. File formats such as Parquet, JSON, ProtoBuf, XML, AVRO and NoSQL databases are all examples of self-describing data. Some of these data formats also dynamic and complex in that every record in the data can have its own set of columns/attributes and each column can be semi-structured/nested.

## How does Drill support queries on self-describing data?

Drill enables queries on self-describing data using the fundamental architectural foundations:

* Dynamic schema discovery or late binding:  Drill allows performing queries directly on self-describing data such as Files, HBase without defining overlay schema definitions in Hive metastore.  The schema is discovered on the fly at the query time. With the dynamic schema discovery, Drill makes it easy to support dynamic and rapidly evolving data models.
* Flexible data model:  Drill is built from the ground up for complex/semi-structured data commonly seen in Hadoop/NoSQL systems. Drill provides intuitive extensions to SQL to represent and operate on complex data. The internal data model of Drill is hierarchical and columnar with which it can represent and perform efficient SQL processing on complex data natively without flattening into rows either at the design time or runtime.

Together with the dynamic data discovery and a flexible data model that can handle complex data types, Drill allows users to get fast and complete value from all their data.

## But I already have schemas defined in Hive metastore? Can I use that with Drill?

Yes, Hive also serves as data source for Drill. So you can simply point to the Hive metastore from Drill and start performing low latency queries on Hive tables with no modifications.

## Is Drill trying to be "anti-schema" or "anti-DBA"?

Of course not! Central EDW schemas work great if data models are not changing often, value of data is well understood and is ready to be operationalized for regular reporting purposes. However, during data exploration and discovery phase, rigid modeling requirement poses challenges and delays value from data, especially in the Hadoop/NoSQL environments where the data is highly complex, dynamic and evolving fast. Few challenges include

* Complex data models (eg: JSON)  are hard to map to relational paradigms
* Centralized schemas are hard to keep up with when data models evolve fast
* Static models defined for known questions are not enough for the diversity and volumes of big data
* Non-repetitive/ad hoc queries and short-term data exploration needs may not justify modeling costs

Drill is all about flexibility. The flexible schema management capabilities in Drill lets users explore the data in its native format as it comes in directly and create models/structure if needed in Hive metastore or using the CREATE TABLE/CREATE VIEW syntax within Drill.

## What does a Drill query look like?

Drill uses a de-centralized metadata model and relies on its storage plugins to provide with the metadata. Drill supports queries on file system (distributed and local), HBase and Hive tables. There is a storage plugin associated with each data source that is supported by Drill.

Here is the anatomy of a Drill query.

![]({{ site.baseurl }}/images/overview-img1.png)

## Can I connect to Drill from my BI tools (Tableau, MicroStrategy, etc.)?

Yes, Drill provides JDBC/ODBC drivers for integrating with BI/SQL based tools.

## What SQL functionality can Drill support?

Drill provides ANSI standard SQL (not SQL "Like" or Hive QL) with support for all key analytics functionality such as SQL data types, joins, aggregations, filters, sort, sub-queries (including correlated), joins in where clause etc. [Click here](https://cwiki.apache.org/confluence/display/DRILL/SQL+Overview) for reference on SQL functionality in Drill.

## What Hadoop distributions does Drill work with?

Drill is not designed with a particular Hadoop distribution in mind and we expect it to work with all Hadoop distributions that support Hadoop 2.3.x+ API. We have validated it so far with Apache Hadoop/MapR/CDH/Amazon EMR distributions (Amazon EMR requires a custom configuration required - contact <drill-user@incubator.apache.org> for questions.

## How does Drill achieve performance?

Drill is built from the ground up for performance on large-scale datasets. The key architectural components that help in achieving performance include. 

* Distributed query optimization & execution
* Columnar execution
* Vectorization
* Runtime compilation & code generation
* Optimistic/pipelined execution

## Does Drill support multi-tenant/high concurrency environments?

Drill is built to support several 100s of queries at any given point. Clients can submit requests to any node running Drillbit service in the cluster (no master-slave concept). To support more users, you simply have to add more nodes to the cluster.

## Do I need to load data into Drill to start querying it?

No. Drill can query data "in situ".
    
## What is the best way to get started with Drill?

The best way to get started is to just try it out. It just takes a few minutes even if you do not have a cluster. Here is a good place to start: [Apache Drill in 10 minutes](https://cwiki.apache.org/confluence/display/DRILL/Apache+Drill+in+10+Minutes).

## How can I ask questions and provide feedback?

Please post your questions and feedback on <drill-user@incubator.apache.org>. We are happy to have you try out Drill and help with any questions!

## How can I contribute to Drill?

Please refer to the [Get Involved]({{ site.baseurl }}/community/#getinvolved) page on how to get involved with Drill.