---
title: "Flexibility"
parent: "Architectural Highlights"
---
The Drill architecture brings the SQL Ecosystem and *Performance* of
the relational systems to Hadoop scale data *without* compromising 
the *flexibility* of Hadoop/NoSQL systems. There are several core
architectural elements in Apache Drill that make it a highly flexible and
efficient query engine.

The following features contribute to Drill's flexible architecture:

**_Dynamic schema discovery_**

Drill does not require schema or type specification for the data in order to
start the query execution process. Instead, Drill starts processing the data
in units called record-batches and discovers the schema on the fly during
processing. Self-describing data formats such as Parquet, JSON, AVRO, and
NoSQL databases have schema specified as part of the data itself, which Drill
leverages dynamically at query time. Schema can change over the course of a
Drill query, so all of the Drill operators are designed to reconfigure
themselves when such schema changing events occur.

**_Flexible data model_**

Drill is purpose-built from the ground up for complex/multi-structured data
commonly seen in Hadoop/NoSQL applications such as social/mobile, clickstream,
logs, and sensor equipped IOT. From a user point of view, Drill allows access
to nested data attributes, just like SQL columns, and provides intuitive
extensions to easily operate on them. From an architectural point of view,
Drill provides a flexible hierarchical columnar data model that can represent
complex, highly dynamic and evolving data models, and allows for efficient
processing of it without the need to flatten or materialize it at design time
or at execution time. Relational data in Drill is treated as a special or
simplified case of complex/multi-structured data.

**_De-centralized metadata_**

Unlike other SQL-on-Hadoop technologies or any traditional relational
database, Drill does not have a centralized metadata requirement. In order to
query data through Drill, users do not need to create and manage tables and
views in a metadata repository, or rely on a database administrator group for
such a function.

Drill metadata is derived from the storage plugins that correspond to data
sources. Drill supports a varied set of storage plugins that provide a
spectrum of metadata ranging from full metadata such as for Hive, partial
metadata such as for HBase, or no central metadata such as for files.

De-centralized metadata also means that Drill is NOT tied to a single Hive
repository. Users can query multiple Hive repositories at once and then
combine the data with information from HBase tables or with a file in a
distributed file system.

Users also have the ability to create metadata (tables/views/databases) within
Drill using the SQL DDL syntax. De-centralized metadata is applicable during
metadata creation. Drill allows persisting metadata in one of the underlying
data sources.

From a client access perspective, Drill metadata is organized just like a
traditional DB (Databases->Tables/Views->Columns). The metadata is accessible
through the ANSI standard INFORMATION_SCHEMA database

For more information on how to configure and work various data sources with
Drill, refer to [Connect Apache Drill to Data Sources]({{ site.baseurl }}/docs/connect-a-data-source-introduction).

**_Extensibility_**

Drill provides an extensible architecture at all layers, including the storage
plugin, query, query optimization/execution, and client API layers. You can
customize any layer for the specific needs of an organization or you can
extend the layer to a broader array of use cases.

Drill provides a built in classpath scanning and plugin concept to add
additional storage plugins, functions, and operators with minimal
configuration.

The following list provides a few examples of Drill’s extensible architectural
capabilities:

* A high performance Java API to implement custom UDFs/UDAFs
* Ability to go beyond Hadoop by implementing custom storage plugins to other data sources such as Oracle/MySQL or NoSQL stores, such as Mongo or Cassandra
* An API to implement custom operators
* Support for direct execution of strongly specified JSON based logical and physical plans to help with the simplification of testing, and to enable integration of alternative query languages other than SQL.