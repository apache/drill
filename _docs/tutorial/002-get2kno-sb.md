---
title: "Getting to Know the Drill Sandbox"
parent: "Apache Drill Tutorial"
---
This section describes the configuration of the Apache Drill system that you
have installed and introduces the overall use case for the tutorial.

# Storage Plugins Overview

The Hadoop cluster within the sandbox is set up with MapR-FS, MapR-DB, and
Hive, which all serve as data sources for Drill in this tutorial. Before you
can run queries against these data sources, Drill requires each one to be
configured as a storage plugin. A storage plugin defines the abstraction on
the data sources for Drill to talk to and provides interfaces to read/write
and get metadata from the data source. Each storage plugin also exposes
optimization rules for Drill to leverage for efficient query execution.

Take a look at the pre-configured storage plugins by opening the Drill Web UI.

Feel free to skip this section and jump directly to the queries: [Lesson 1:
Learn About the Data
Set](/drill/docs/lession-1-learn-about-the-data-set)

  * Launch a web browser and go to: `http://<IP address of the sandbox>:8047`
  * Go to the Storage tab
  * Open the configured storage plugins one at a time by clicking Update
  * You will see the following plugins configured.

## maprdb

A storage plugin configuration for MapR-DB in the sandbox. Drill uses a single
storage plugin for connecting to HBase as well as MapR-DB, which is an
enterprise grade in-Hadoop NoSQL database. In addition to the following brief example, see the [Registering HBase](/drill/docs/registering-hbase) for more
information on how to configure Drill to query HBase.

    {
      "type" : "hbase",
      "enabled" : true,
      "config" : {
        "hbase.table.namespace.mappings" : "*:/tables"
      }
     }

## dfs

This is a storage plugin configuration for the MapR file system (MapR-FS) in
the sandbox. The connection attribute indicates the type of distributed file
system: in this case, MapR-FS. Drill can work with any distributed system,
including HDFS, S3, and so on.

The configuration also includes a set of workspaces; each one represents a
location in MapR-FS:

  * root: access to the root file system location
  * clicks: access to nested JSON log data
  * logs: access to flat (non-nested) JSON log data in the logs directory and its subdirectories
  * views: a workspace for creating views

A workspace in Drill is a location where users can easily access a specific
set of data and collaborate with each other by sharing artifacts. Users can
create as many workspaces as they need within Drill.

Each workspace can also be configured as “writable” or not, which indicates
whether users can write data to this location and defines the storage format
in which the data will be written (parquet, csv, json). These attributes
become relevant when you explore SQL commands, especially CREATE TABLE
AS (CTAS) and CREATE VIEW.

Drill can query files and directories directly and can detect the file formats
based on the file extension or the first few bits of data within the file.
However, additional information around formats is required for Drill, such as
delimiters for text files, which are specified in the “formats” section below.

    {
      "type": "file",
      "enabled": true,
      "connection": "maprfs:///",
      "workspaces": {
        "root": {
          "location": "/mapr/demo.mapr.com/data",
          "writable": false,
          "storageformat": null
        },
        "clicks": {
          "location": "/mapr/demo.mapr.com/data/nested",
          "writable": true,
          "storageformat": "parquet"
        },
        "logs": {
          "location": "/mapr/demo.mapr.com/data/flat",
          "writable": true,
          "storageformat": "parquet"
        },
        "views": {
          "location": "/mapr/demo.mapr.com/data/views",
          "writable": true,
          "storageformat": "parquet"
     },
     "formats": {
       "psv": {
         "type": "text",
         "extensions": [
           "tbl"
         ],
         "delimiter": "|"
     },
     "csv": {
       "type": "text",
       "extensions": [
         "csv"
       ],
       "delimiter": ","
     },
     "tsv": {
       "type": "text",
       "extensions": [
         "tsv"
       ],
       "delimiter": "\t"
     },
     "parquet": {
       "type": "parquet"
     },
     "json": {
       "type": "json"
     }
    }}

## hive

A storage plugin configuration for a Hive data warehouse within the sandbox.
Drill connects to the Hive metastore by using the configured metastore thrift
URI. Metadata for Hive tables is automatically available for users to query.

     {
      "type": "hive",
      "enabled": true,
      "configProps": {
        "hive.metastore.uris": "thrift://localhost:9083",
        "hive.metastore.sasl.enabled": "false"
      }
    }

# Client Application Interfaces

Drill also provides additional application interfaces for the client tools to
connect and access from Drill. The interfaces include the following.

### ODBC/JDBC drivers

Drill provides ODBC/JDBC drivers to connect from BI tools such as Tableau,
MicroStrategy, SQUirrel, and Jaspersoft; refer to [Using ODBC to Access Apache
Drill from BI Tools](/drill/docs/odbc-jdbc-interfaces/using-odbc-to- access-apache-drill-from-bi-tools) and [Using JDBC to Access Apache Drill](/drill/docs/odbc-jdbc-interfaces#using-jdbc-to-access-apache-drill-from-squirrel) to learn
more.

### SQLLine

SQLLine is a JDBC application that comes packaged with Drill. In order to
start working with it, you can use the command line on the demo cluster to log
in as root, then enter `sqlline`. Use `mapr` as the login password. For
example:

    $ ssh root@localhost -p 2222
    Password:
    Last login: Mon Sep 15 13:46:08 2014 from 10.250.0.28
    Welcome to your Mapr Demo virtual machine.
    [root@maprdemo ~]# sqlline
    sqlline version 1.1.6
    0: jdbc:drill:>

### Drill Web UI

The Drill Web UI is a simple user interface for configuring and manage Apache
Drill. This UI can be launched from any of the nodes in the Drill cluster. The
configuration for Drill includes setting up storage plugins that represent the
data sources on which Drill performs queries. The sandbox comes with storage
plugins configured for the Hive, HBase, MapR file system, and local file
system.

Users and developers can get the necessary information for tuning and
performing diagnostics on queries, such as the list of queries executed in a
session and detailed query plan profiles for each.

Detailed configuration and management of Drill is out of scope for this
tutorial.

The Web interface for Apache Drill also provides a query UI where users can
submit queries to Drill and observe results. Here is a screen shot of the Web
UI for Apache Drill:

![drill query flow]({{ site.baseurl }}/docs/img/DrillWebUI.png)

### REST API

Drill provides a simple REST API for the users to query data as well as manage
the system. The Web UI leverages the REST API to talk to Drill.

This tutorial introduces sample queries that you can run by using SQLLine.
Note that you can run the queries just as easily by launching the Drill Web
UI. No additional installation or configuration is required.

# Use Case Overview

As you run through the queries in this tutorial, put yourself in the shoes of
an analyst with basic SQL skills. Let us imagine that the analyst works for an
emerging online retail business that accepts purchases from its customers
through both an established web-based interface and a new mobile application.

The analyst is data-driven and operates mostly on the business side with
little or no interaction with the IT department. Recently the central IT team
has implemented a Hadoop-based infrastructure to reduce the cost of the legacy
database system, and most of the DWH/ETL workload is now handled by
Hadoop/Hive. The master customer profile information and product catalog are
managed in MapR-DB, which is a NoSQL database. The IT team has also started
acquiring clickstream data that comes from web and mobile applications. This
data is stored in Hadoop as JSON files.

The analyst has a number of data sources that he could explore, but exploring
them in isolation is not the way to go. There are some potentially very
interesting analytical connections between these data sources. For example, it
would be good to be able to analyze customer records in the clickstream data
and tie them to the master customer data in MapR DB.

The analyst decides to explore various data sources and he chooses to do that
by using Apache Drill. Think about the flexibility and analytic capability of
Apache Drill as you work through the tutorial.

# What's Next

Start running queries by going to [Lesson 1: Learn About the Data
Set](/drill/docs/lession-1-learn-about-the-data-set).

