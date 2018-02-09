---
title: "SHOW TABLES"
date: 2018-02-09 00:16:06 UTC
parent: "SQL Commands"
---
The SHOW TABLES command returns a list of views created within a schema. It
also returns the tables that exist in Hive and HBase when you use storage plugin configurations for these data sources. See [Storage Plugin
Registration]({{ site.baseurl }}/docs/storage-plugin-registration).

## Syntax

The SHOW TABLES command supports the following syntax:

    SHOW TABLES;

## Usage Notes

First issue the USE command to identify the schema for which you want to view
tables or views. For example, the following USE statement tells Drill that you
only want information from the `dfs.myviews` schema:

    USE dfs.myviews;

In this example, “`myviews`” is a workspace created within the
`dfs` storage plugin configuration.

When you use a particular schema and then issue the SHOW TABLES command, Drill
returns the tables and views within that schema.

## Limitations

  * You can create and query tables within the file system, however Drill does not return these tables when you issue the SHOW TABLES command. You can issue the [SHOW FILES ]({{ site.baseurl }}/docs/show-files-command)command to see a list of all files, tables, and views, including those created in Drill. 

  * You cannot create Hive or HBase tables in Drill. 

## Examples

The following examples demonstrate the steps that you can follow when you want
to issue the SHOW TABLES command on the file system, Hive, and HBase.  
  
Complete the following steps to see views that exist in a file system and
tables that exist in Hive and HBase data sources:

  1. Issue the SHOW SCHEMAS command to see a list of available schemas.

        0: jdbc:drill:zk=drilldemo:5181> show schemas;
        +-------------+
        | SCHEMA_NAME |
        +-------------+
        | hive.default |
        | dfs.reviews |
        | dfs.flatten |
        | dfs.default |
        | dfs.root  |
        | dfs.logs  |
        | dfs.myviews   |
        | dfs.clicks  |
        | dfs.tmp   |
        | sys       |
        | hbase     |
        | INFORMATION_SCHEMA |
        | s3.twitter  |
        | s3.reviews  |
        | s3.default  |
        +-------------+
        15 rows selected (0.072 seconds)

  2. Issue the USE command to switch to a particular schema. When you use a particular schema, Drill searches or queries within that schema only. 

        0: jdbc:drill:zk=drilldemo:5181> use dfs.myviews;
        +------------+------------+
        |   ok  |  summary   |
        +------------+------------+
        | true      | Default schema changed to 'dfs.myviews' |
        +------------+------------+
        1 row selected (0.025 seconds)

  3. Issue the SHOW TABLES command to see the views or tables that exist within workspace.

        0: jdbc:drill:zk=drilldemo:5181> show tables;
        +--------------+------------+
        | TABLE_SCHEMA | TABLE_NAME |
        +--------------+------------+
        | dfs.myviews   | logs_vw   |
        | dfs.myviews   | customers_vw |
        | dfs.myviews   | s3_review_vw |
        | dfs.myviews   | clicks_vw  |
        | dfs.myviews   | nestedclickview |
        | dfs.myviews   | s3_user_vw |
        | dfs.myviews   | s3_bus_vw  |
        +--------------+------------+
        7 rows selected (0.499 seconds)
        0: jdbc:drill:zk=drilldemo:5181>

  4. Switch to the Hive schema and issue the SHOW TABLES command to see the Hive tables that exist.

        0: jdbc:drill:zk=drilldemo:5181> use hive;
        +------------+------------+
        |   ok  |  summary   |
        +------------+------------+
        | true      | Default schema changed to 'hive' |
        +------------+------------+
        1 row selected (0.043 seconds)
         
        0: jdbc:drill:zk=drilldemo:5181> show tables;
        +--------------+------------+
        | TABLE_SCHEMA | TABLE_NAME |
        +--------------+------------+
        | hive.default | orders     |
        | hive.default | products   |
        +--------------+------------+
        2 rows selected (0.552 seconds)

  5. Switch to the HBase schema and issue the SHOW TABLES command to see the HBase tables that exist within the schema.

        0: jdbc:drill:zk=drilldemo:5181> use hbase;
        +------------+------------+
        |   ok  |  summary   |
        +------------+------------+
        | true      | Default schema changed to 'hbase' |
        +------------+------------+
        1 row selected (0.043 seconds)
         
         
        0: jdbc:drill:zk=drilldemo:5181> show tables;
        +--------------+------------+
        | TABLE_SCHEMA | TABLE_NAME |
        +--------------+------------+
        | hbase     | customers  |
        +--------------+------------+
        1 row selected (0.412 seconds)

  

  

