---
title: "Connect to a Data Source"
---
[Previous](/docs/installing-drill-in-distributed-mode)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Back to Table of Contents](/docs)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Next](/docs/storage-plugin-registration)

A storage plugin is an interface for connecting to a data source to read and write data. Apache Drill connects to a data source, such as a file on the file system or a Hive metastore, through a storage plugin. When you execute a query, Drill gets the plugin name you provide in FROM clause of your query or from the default you specify in the USE.<plugin name> command that precedes the query.
. 

In addition to the connection string, the storage plugin configures the workspace and file formats for reading data, as described in subsequent sections. 

## Storage Plugins Internals
The following image represents the storage plugin layer between Drill and a
data source:

![drill query flow]({{ site.baseurl }}/docs/img/storageplugin.png)

A storage plugin provides the following information to Drill:

  * Metadata available in the underlying data source
  * Location of data
  * Interfaces that Drill can use to read from and write to data sources
  * A set of storage plugin optimization rules that assist with efficient and faster execution of Drill queries, such as pushdowns, statistics, and partition awareness

A storage plugin performs scanner and writer functions, and informs the metadata repository of any known metadata. The metadata repository is a database created to store metadata. The metadata is information about the structures that contain the actual data, such as:

  * Schema
  * File size
  * Data ordering
  * Secondary indices
  * Number of blocks

A storage plugin informs the execution engine of any native capabilities, such
as predicate pushdown, joins, and SQL.

