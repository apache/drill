---
title: "Architecture Introduction"
parent: "Architecture"
---
Apache Drill is a low latency distributed query engine for large-scale
datasets, including structured and semi-structured/nested data. Inspired by
Google’s Dremel, Drill is designed to scale to several thousands of nodes and
query petabytes of data at interactive speeds that BI/Analytics environments
require.

Drill is also useful for short, interactive ad-hoc queries on large-scale data sets. Drill is capable of querying nested data in formats like JSON and Parquet and
performing dynamic schema discovery. Drill does not require a centralized
metadata repository.

## High-Level Architecture

Drill includes a distributed execution environment, purpose built for large-
scale data processing. At the core of Apache Drill is the ‘Drillbit’ service,
which is responsible for accepting requests from the client, processing the
queries, and returning results to the client.

A Drillbit service can be installed and run on all of the required nodes in a
Hadoop cluster to form a distributed cluster environment. When a Drillbit runs
on each data node in the cluster, Drill can maximize data locality during
query execution without moving data over the network or between nodes. Drill
uses ZooKeeper to maintain cluster membership and health-check information.

Though Drill works in a Hadoop cluster environment, Drill is not tied to
Hadoop and can run in any distributed cluster environment. The only pre-
requisite for Drill is Zookeeper.

## Query Flow in Drill

The following image represents the flow of a Drill query:
 
![drill query flow]({{ site.baseurl }}/docs/img/queryFlow.png)

The flow of a Drill query typically involves the following steps:

  1. The Drill client issues a query. Any Drillbit in the cluster can accept queries from clients. There is no master-slave concept.
  2. The Drillbit then parses the query, optimizes it, and generates an optimized distributed query plan for fast and efficient execution.
  3. The Drillbit that accepts the query becomes the driving Drillbit node for the request. It gets a list of available Drillbit nodes in the cluster from ZooKeeper. The driving Drillbit determines the appropriate nodes to execute various query plan fragments to maximize data locality.
  4. The Drillbit schedules the execution of query fragments on individual nodes according to the execution plan.
  5. The individual nodes finish their execution and return data to the driving Drillbit.
  6. The driving Drillbit returns results back to the client.

## Drill Clients

You can access Drill through the following interfaces:

  * [Drill shell (SQLLine)]({{ site.baseurl }}/docs/install-drill)
  * [Drill Web UI]({{ site.baseurl }}/docs/monitoring-and-canceling-queries-in-the-drill-web-ui)
  * [ODBC/JDBC]({{ site.baseurl }}/docs/odbc-jdbc-interfaces/#using-odbc-to-access-apache-drill-from-bi-tools) 
  * C++ API

### **_Dynamic schema discovery_**

Drill does not require schema or type specification for data in order to start
the query execution process. Drill starts data processing in record-batches
and discovers the schema during processing. Self-describing data formats such
as Parquet, JSON, AVRO, and NoSQL databases have schema specified as part of
the data itself, which Drill leverages dynamically at query time. Because
schema can change over the course of a Drill query, all Drill operators are
designed to reconfigure themselves when schemas change.

### **_Flexible data model_**

Drill allows access to nested data attributes, just like SQL columns, and
provides intuitive extensions to easily operate on them. From an architectural
point of view, Drill provides a flexible hierarchical columnar data model that
can represent complex, highly dynamic and evolving data models. Drill allows
for efficient processing of these models without the need to flatten or
materialize them at design time or at execution time. Relational data in Drill
is treated as a special or simplified case of complex/multi-structured data.

### **_De-centralized metadata_**

Drill does not have a centralized metadata requirement. You do not need to
create and manage tables and views in a metadata repository, or rely on a
database administrator group for such a function. Drill metadata is derived
from the storage plugins that correspond to data sources. Storage plugins
provide a spectrum of metadata ranging from full metadata (Hive), partial
metadata (HBase), or no central metadata (files). De-centralized metadata
means that Drill is NOT tied to a single Hive repository. You can query
multiple Hive repositories at once and then combine the data with information
from HBase tables or with a file in a distributed file system. You can also
use SQL DDL syntax to create metadata within Drill, which gets organized just
like a traditional database. Drill metadata is accessible through the ANSI
standard INFORMATION_SCHEMA database.

### **_Extensibility_**

Drill provides an extensible architecture at all layers, including the storage
plugin, query, query optimization/execution, and client API layers. You can
customize any layer for the specific needs of an organization or you can
extend the layer to a broader array of use cases. Drill provides a built in
classpath scanning and plugin concept to add additional storage plugins,
functions, and operators with minimal configuration.