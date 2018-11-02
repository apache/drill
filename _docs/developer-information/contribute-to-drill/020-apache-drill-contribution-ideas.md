---
title: "Apache Drill Contribution Ideas"
date: 2018-11-02
parent: "Contribute to Drill"
---
  * Fixing JIRAs
  * SQL functions 
  * Support for new file format readers/writers
  * Support for new data sources
  * New query language parsers
  * Application interfaces
    * BI Tool testing
  * General CLI improvements 
  * Eco system integrations
    * MapReduce
    * Hive views
    * YARN
    * Spark
    * Hue
    * Phoenix

## Fixing JIRAs

This is a good place to begin if you are new to Drill. Feel free to pick
issues from the Drill JIRA list. When you pick an issue, assign it to
yourself, inform the team, and start fixing it.

For any questions, seek help from the team through the [mailing list](http://drill.apache.org/community/#mailinglists).

[https://issues.apache.org/jira/browse/DRILL/?selectedTab=com.atlassian.jira
.jira-projects-plugin:summary-panel](https://issues.apache.org/jira/browse/DRILL/?selectedTab=com.atlassian.jira
.jira-projects-plugin:summary-panel)

## SQL functions

One of the next simple places to start is to implement a DrillFunc. DrillFuncs
is way that Drill express all scalar functions (UDF or system).  First you can
put together a JIRA for one of the DrillFunc's we don't yet have but should
(referencing the capabilities of something like Postgres or SQL Server or your
own use case). Then try to implement one.

One example DrillFunc:  
[ComparisonFunctions.java](https://github.com/apache/drill/blob/3f93454f014196a4da198ce012b605b70081fde0/exec/java-exec/src/main/codegen/templates/ComparisonFunctions.java)
** **

**Additional ideas on functions that can be added to SQL support**

  * Madlib integration
  * Machine learning functions
  * Approximate aggregate functions (such as what is available in BlinkDB)

## Support for new file format readers/writers

Currently Drill supports text, JSON and Parquet file formats natively when
interacting with file system. More readers/writers can be introduced by
implementing custom storage plugins. Example formats are.

  * Sequence
  * RC
  * ORC
  * Protobuf
  * XML
  * Thrift

## Support for new data sources

Writing a new file-based storage plugin, such as a JSON or text-based storage plugin, simply involves implementing a couple of interfaces. The JSON storage plugin is a good example. 

You can refer to the github commits to the mongo db and hbase storage plugin for implementation details: 

* [mongodb_storage_plugin](https://github.com/apache/drill/commit/2ca9c907bff639e08a561eac32e0acab3a0b3304)
* [hbase_storage_plugin](https://github.com/apache/drill/commit/3651182141b963e24ee48db0530ec3d3b8b6841a)

Focus on implementing/extending this list of classes and the corresponding implementations done by Mongo and Hbase. Ignore the mongo db plugin optimizer rules for pushing predicates into the scan.

Initially, concentrate on basics:

* AbstractGroupScan (MongoGroupScan, HbaseGroupScan)  
* SubScan (MongoSubScan, HbaseSubScan)  
* RecordReader (MongoRecordReader, HbaseRecordReader)  
* BatchCreator (MongoScanBatchCreator, HbaseScanBatchCreator)  
* AbstractStoragePlugin (MongoStoragePlugin, HbaseStoragePlugin)  
* StoragePluginConfig (MongoStoragePluginConfig, HbaseStoragePluginConfig)

Implement custom storage plugins for the following non-Hadoop data sources:

  * NoSQL databases (such as Mongo, Cassandra, Couch etc)
  * Search engines (such as Solr, Lucidworks, Elastic Search etc)
  * SQL databases (MySQL< PostGres etc)
  * Generic JDBC/ODBC data sources
  * HTTP URL
  * \----

## New query language parsers

Drill exposes strongly typed JSON APIs for logical and physical plans. Drill provides a
SQL language parser today, but any language parser that can generate
logical/physical plans can use Drill's power on the backend as the distributed
low latency query execution engine along with its support for self-describing
data and complex/multi-structured data.

  * Pig parser : Use Pig as the language to query data from Drill. Great for existing Pig users.
  * Hive parser : Use HiveQL as the language to query data from Drill. Great for existing Hive users.

## Application interfaces

Drill currently provides JDBC/ODBC drivers for the applications to interact
along with a basic version of REST API and a C++ API. The following list
provides a few possible application interface opportunities:

  * Enhancements to REST APIs (<https://issues.apache.org/jira/browse/DRILL-77>)
  * Expose Drill tables/views as REST APIs
  * Language drivers for Drill (python etc)
  * Thrift support
  * ....

### BI Tool testing

Drill provides JDBC/ODBC drivers to connect to BI tools. We need to make sure
Drill works with all major BI tools. Doing a quick sanity testing with your
favorite BI tool is a good place to learn Drill and also uncover issues in
being able to do so.

## General CLI improvements

Currently Drill uses SQLLine as the CLI. The goal of this effort is to improve
the CLI experience by adding functionality such as execute statements from a
file, output results to a file, display version information, and so on.

## Eco system integrations

### MapReduce

Allow using result set from Drill queries as input to the Hadoop/MapReduce
jobs.

### Hive views

Query data from existing Hive views using Drill queries. Drill needs to parse
the HiveQL and translate them appropriately (into Drill's SQL or
logical/physical plans) to execute the requests.

### YARN

[https://issues.apache.org/jira/browse/_DRILL_-1170](https://issues.apache.org
/jira/browse/DRILL-1170)

## Spark

Provide ability to invoke Drill queries as part of Apache Spark programs. This
gives ability for Spark developers/users to leverage Drill richness of the
query layer , for data source access and as low latency execution engine.

### Hue

Hue is a GUI for users to interact with various Hadoop eco system components
(such as Hive, Oozie, Pig, HBase, Impala ...). The goal of this project is to
expose Drill as an application inside Hue so users can explore Drill metadata
and do SQL queries.

### Phoenix

Phoenix provides a low latency query layer on HBase for operational
applications. The goal of this effort is to explore opportunities for
integrating Phoenix with Drill.

