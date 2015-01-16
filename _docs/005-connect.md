---
title: "Connect to Data Sources"
---
Apache Drill serves as a query layer that connects to data sources through
storage plugins. Drill uses the storage plugins to interact with data sources.
You can think of a storage plugin as a connection between Drill and a data
source.

The following image represents the storage plugin layer between Drill and a
data source:

![drill query flow]({{ site.baseurl }}/docs/img/storageplugin.png)

Storage plugins provide the following information to Drill:

  * Metadata available in the underlying data source
  * Location of data
  * Interfaces that Drill can use to read from and write to data sources
  * A set of storage plugin optimization rules that assist with efficient and faster execution of Drill queries, such as pushdowns, statistics, and partition awareness

Storage plugins perform scanner and writer functions, and inform the metadata
repository of any known metadata, such as:

  * Schema
  * File size
  * Data ordering
  * Secondary indices
  * Number of blocks

Storage plugins inform the execution engine of any native capabilities, such
as predicate pushdown, joins, and SQL.

Drill provides storage plugins for files and HBase/M7. Drill also integrates
with Hive through a storage plugin. Hive provides a metadata abstraction layer
on top of files and HBase/M7.

When you run Drill to query files in HBase/M7, Drill can perform direct
queries on the data or go through Hive, if you have metadata defined there.
Drill integrates with the Hive metastore for metadata and also uses a Hive
SerDe for the deserialization of records. Drill does not invoke the Hive
execution engine for any requests.