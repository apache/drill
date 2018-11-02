---
title: "Architecture Introduction"
date: 2018-11-02
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
scale data processing. At the core of Apache Drill is the "Drillbit" service,
which is responsible for accepting requests from the client, processing the
queries, and returning results to the client.

A Drillbit service can be installed and run on all of the required nodes in a
Hadoop cluster to form a distributed cluster environment. When a Drillbit runs
on each data node in the cluster, Drill can maximize data locality during
query execution without moving data over the network or between nodes. Drill
uses ZooKeeper to maintain cluster membership and health-check information.

Though Drill works in a Hadoop cluster environment, Drill is not tied to
Hadoop and can run in any distributed cluster environment. The only pre-requisite for Drill is ZooKeeper.

See [Drill Query Execution]({{ site.baseurl }}/docs/drill-query-execution/).

## Drill Clients

You can access Drill through the following interfaces:

  * [Drill shell]({{ site.baseurl }}/docs/configuring-the-drill-shell/)
  * [Drill Web Console]({{ site.baseurl }}/docs/monitoring-and-canceling-queries-in-the-drill-web-console)
  * [ODBC/JDBC]({{ site.baseurl }}/docs/interfaces-introduction/#using-odbc-to-access-apache-drill-from-bi-tools) 
  * C++ API

### **_Dynamic schema discovery_**

Drill does not require schema or type specification for data in order to start
the query execution process. Drill starts data processing in record-batches
and discovers the schema during processing. Self-describing data formats such
as Parquet, JSON, AVRO, and NoSQL databases have schema specified as part of
the data itself, which Drill leverages dynamically at query time. Because the
schema can change over the course of a Drill query, many Drill operators are
designed to reconfigure themselves when schemas change.

### **_Flexible data model_**

Drill allows access to nested data attributes, as if they were SQL columns, and
provides intuitive extensions to easily operate on them. From an architectural
point of view, Drill provides a flexible hierarchical columnar data model that
can represent complex, highly dynamic and evolving data models. Relational data in Drill
is treated as a special or simplified case of complex/multi-structured data.

### **_No centralized metadata_**

Drill does not have a centralized metadata requirement. You do not need to
create and manage tables and views in a metadata repository, or rely on a
database administrator group for such a function. Drill metadata is derived
through the storage plugins that correspond to data sources. Storage plugins
provide a spectrum of metadata ranging from full metadata (Hive), partial
metadata (HBase), or no central metadata (files). De-centralized metadata
means that Drill is NOT tied to a single Hive repository. You can query
multiple Hive repositories at once and then combine the data with information
from HBase tables or with a file in a distributed file system. You can also
use SQL DDL statements to create metadata within Drill, which gets organized just
like a traditional database. Drill metadata is accessible through the ANSI
standard INFORMATION_SCHEMA database.

### **_Extensibility_**

Drill provides an extensible architecture at all layers, including the storage
plugin, query, query optimization/execution, and client API layers. You can
customize any layer for the specific needs of an organization or you can
extend the layer to a broader array of use cases. Drill uses 
classpath scanning to find and load plugins, and to add additional storage plugins,
functions, and operators with minimal configuration.
