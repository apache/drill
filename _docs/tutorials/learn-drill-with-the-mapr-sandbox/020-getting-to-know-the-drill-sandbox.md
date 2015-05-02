---
title: "Getting to Know the Drill Sandbox"
parent: "Learn Drill with the MapR Sandbox"
---
This section covers key information about the Apache Drill tutorial. After [installing the Drill sandbox]({{ site.baseurl }}/docs/installing-the-apache-drill-sandbox) and starting the sandbox, you can open another terminal window (Linux) or Command Prompt (Windows) and use the secure shell (ssh) to connect to the VM, assuming ssh is installed. Use the following login name and password: mapr/mapr. For
example:

    $ ssh mapr@localhost -p 2222
    Password:
    Last login: Mon Sep 15 13:46:08 2014 from 10.250.0.28
    Welcome to your Mapr Demo virtual machine.

Using the secure shell instead of the VM interface has some advantages. You can copy/paste commands from the tutorial and avoid mouse control problems.

Drill includes SQLLine, a JDBC utility for connecting to relational databases and executing SQL commands. `SQLLine` is a sql client on the sandbox that starts Drill only in embedded mode. After logging into the sandbox,  use the `SQLLine` command to start SQLLine for executing Drill queries in embedded mode.  

    [mapr@maprdemo ~]# sqlline
    sqlline version 1.1.6
    0: jdbc:drill:>

In distributed mode, [Warden](http://doc.mapr.com/display/MapR/Apache+Drill+Installation+Overview) attempts to start Drill automatically when Drill is defined as service.

[Starting SQLLine outside the sandbox]({{ site.baseurl }}/docs/starting-stopping-drill) for use with Drill requires entering more options than are shown here. When you type sqlline on the Sandbox command line, a script runs that includes startup options shown in the section, ["Starting/Stopping Drill"](http://apache.github.io/drill/docs/starting-stopping-drill/#invoking-sqlline/connecting-to-a-schema).

In this tutorial you query a number of data sets, including Hive and HBase, and files on the file system, such as CSV, JSON, and Parquet files. To access these diverse data sources, you connect Drill to storage plugins. 

## Storage Plugin Overview
This section describes storage plugins included in the sandbox. For general information about Drill storage plugins, see ["Connect to a Data Source"]({{ site.baseurl }}/docs/connect-a-data-source-introduction).
Take a look at the pre-configured storage plugins for the sandbox by opening the Storage tab in the Drill Web UI. Launch a web browser and go to: `http://<IP address>:8047/storage`. For example:

    http://localhost:8047/storage

The control panel for managing storage plugins appears.

![sandbox plugin]({{ site.baseurl }}/docs/img/get2kno_plugin.png)

You see that the following storage plugin controls:

* cp
* dfs
* hive
* maprdb
* hbase
* mongo

Click Update to look at a configuration. 

In some cases, the storage plugin defined for use in the sandbox differs from the default storage plugin of the same name in a Drill installation as described in the following sections. Typically you create a storage plugin or customize an existing one for analyzing a particular data source. 

The tutorial uses the dfs, hive, maprdb, and hbase storage plugins. 

### dfs

The `dfs` storage plugin in the sandbox configures a connection to the MapR file system (MapR-FS). 

The `dfs` storage plugin configuration in the sandbox also includes a set of workspaces; each one represents a
location in MapR-FS:

  * root: access to the root file system location
  * clicks: access to nested JSON log data
  * logs: access to flat (non-nested) JSON log data in the logs directory and its subdirectories
  * views: a workspace for creating views

The `dfs` definition includes format definitions.

    {
      "type": "file",
      "enabled": true,
      "connection": "maprfs:///",
      "workspaces": {
        "root": {
          "location": "/mapr/demo.mapr.com/data",
          "writable": false,
          "defaultinputformat": null
        },
        "clicks": {
          "location": "/mapr/demo.mapr.com/data/nested",
          "writable": true,
          "defaultinputformat": "parquet"
        },
     . . .
     "formats": {
     . . .
       "csv": {
          "type": "text",
          "extensions": [
            "csv"
          ],
         "delimiter": ","
      },
     . . .
      "json": {
       "type": "json"
       }
      }
    }

### maprdb

The maprdb storage plugin is a configuration for MapR-DB in the sandbox. You use this plugin in the sandbox to query HBase as well as MapR-DB data because the sandbox does not include HBase services. In addition to the following brief example, see the [Registering HBase]({{ site.baseurl }}/docs/hbase-storage-plugin) for more
information on how to configure Drill to query HBase.

    {
      "type" : "hbase",
      "enabled" : true,
      "config" : {
        "hbase.table.namespace.mappings" : "*:/tables"
      }
     }

### hive

The hive storage plugin is a configuration for a Hive data warehouse within the sandbox.
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

## Use Case Overview

This section describes the use case that serves as the basis for the tutorial. Imagine being an analyst with basic SQL skills who works for an
emerging online retail business. The business accepts purchases from its customers
through both an established web-based interface and a new mobile application.

Your job is data-driven and independent with little or no interaction with the IT department. Recently the central IT team
has implemented a Hadoop-based infrastructure to reduce the cost of the legacy
database system, and most of the DWH/ETL workload is now handled by
Hadoop/Hive. MapR-DB manages the master customer profile information and product catalog. MapR-DB is a NoSQL database. The IT team has also started
acquiring clickstream data that comes from web and mobile applications. This
data is stored in Hadoop as JSON files.

You have a number of data sources to explore.  For example, analyzing customer records in the clickstream data and tying them to the master customer data in MapR-DB might yield some potentially interesting analytical connections. You decide to explore various data sources by using Apache Drill. You need Apache Drill to provide flexibility and analytic capability.

# What's Next

Start running queries by going to [Lesson 1: Learn About the Data
Set]({{ site.baseurl }}/docs/lession-1-learn-about-the-data-set).

