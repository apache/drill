---
title: "Architectural Overview"
parent: "Apache Drill Documentation"
---
Apache Drill is a low latency distributed query engine for large-scale
datasets, including structured and semi-structured/nested data. Inspired by
Google’s Dremel, Drill is designed to scale to several thousands of nodes and
query petabytes of data at interactive speeds that BI/Analytics environments
require.

### High-Level Architecture

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

### Query Flow in Drill

The following image represents the flow of a Drill query:

![](../img/queryFlow.PNG?version=1&modifica
tionDate=1400017845000&api=v2)  

The flow of a Drill query typically involves the following steps:

  1. The Drill client issues a query. Any Drillbit in the cluster can accept queries from clients. There is no master-slave concept.
  2. The Drillbit then parses the query, optimizes it, and generates an optimized distributed query plan for fast and efficient execution.
  3. The Drillbit that accepts the query becomes the driving Drillbit node for the request. It gets a list of available Drillbit nodes in the cluster from ZooKeeper. The driving Drillbit determines the appropriate nodes to execute various query plan fragments to maximize data locality.
  4. The Drillbit schedules the execution of query fragments on individual nodes according to the execution plan.
  5. The individual nodes finish their execution and return data to the driving Drillbit.
  6. The driving Drillbit returns results back to the client.

### Drill Clients

You can access Drill through the following interfaces:

  * Drill shell (SQLLine)
  * Drill Web UI
  * ODBC 
  * JDBC
  * C++ API

Click on either of the following links to continue reading about Drill's
architecture:

  * [Core Modules within a Drillbit](/confluence/display/DRILL/Core+Modules+within+a+Drillbit)
  * [Architectural Highlights](/confluence/display/DRILL/Architectural+Highlights)