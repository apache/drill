---
title: "Connect a Data Source Introduction"
parent: "Connect a Data Source"
---
A storage plugin provides the following information to Drill:

* Interfaces that Drill can use to read from and write to data sources.   
* A set of storage plugin optimization rules that assist with efficient and faster execution of Drill queries, such as pushdowns, statistics, and partition awareness.  

Through the storage plugin, Drill connects to a data source, such as a database, a file on a local or distributed file system, or a Hive metastore. When you execute a query, Drill gets the plugin name in one of several ways:

* The FROM clause of the query can identify the plugin to use.
* The USE <plugin name> command can precede the query.
* You can specify the storage plugin when starting Drill.

In addition to providing a the connection string to the data source, the storage plugin configures the workspace and file formats for reading data, as described in subsequent sections. 

## Storage Plugins Internals
The following image represents the storage plugin layer between Drill and a
data source:

![drill query flow]({{ site.baseurl }}/docs/img/storageplugin.png)

A storage plugin provides the following information to Drill:

  * Metadata available in the underlying data source
  * Location of data
  * Interfaces that Drill can use to read from and write to data sources
  * A set of storage plugin optimization rules that assist with efficient and faster execution of Drill queries, such as pushdowns, statistics, and partition awareness

A storage plugin performs scanner and writer functions and informs the execution engine of any native capabilities, such
as predicate pushdown, joins, and SQL.