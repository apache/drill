---
title: "Apache Drill Contribution Ideas"
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

For any questions, seek help from the team by sending email to [drill-
dev@incubator.apache.org](mailto:drill-dev@incubator.apache.org).

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
[https://github.com/apache/incubator-
drill/blob/103072a619741d5e228fdb181501ec2f82e111a3/sandbox/prototype/exec
/java-exec/src/main/java/org/apache/drill/exec/expr/fn/impl/ComparisonFunction
s.java](https://github.com/apache/incubator-
drill/blob/103072a619741d5e228fdb181501ec2f82e111a3/sandbox/prototype/exec
/java-exec/src/main/java/org/apache/drill/exec/expr/fn/impl/ComparisonFunction
s.java)** **

**Additional ideas on functions that can be added to SQL support**

  * Madlib integration
  * Machine learning functions
  * Approximate aggregate functions (such as what is available in BlinkDB)

## Support for new file format readers/writers

Currently Drill supports text, JSON and Parquet file formats natively when
interacting with file system. More readers/writers can be introduced by
implementing custom storage plugins. Example formats include below.

  * AVRO
  * Sequence
  * RC
  * ORC
  * Protobuf
  * XML
  * Thrift
  * ....

## Support for new data sources

Implement custom storage plugins for the following non-Hadoop data sources:

  * NoSQL databases (such as Mongo, Cassandra, Couch etc)
  * Search engines (such as Solr, Lucidworks, Elastic Search etc)
  * SQL databases (MySQL< PostGres etc)
  * Generic JDBC/ODBC data sources
  * HTTP URL
  * \----

## New query language parsers

Drill exposes strongly typed JSON APIs for logical and physical plans (plan
syntax at [https://docs.google.com/a/maprtech.com/document/d/1QTL8warUYS2KjldQ
rGUse7zp8eA72VKtLOHwfXy6c7I/edit#heading=h.n9gdb1ek71hf](https://docs.google.com/a/maprtech.com/document/d/1QTL8warUYS2KjldQ
rGUse7zp8eA72VKtLOHwfXy6c7I/edit#heading=h.n9gdb1ek71hf) ). Drill provides a
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

