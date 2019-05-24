---
title: "MapR-DB Format"
date: 2019-05-24
parent: "Connect a Data Source"
---

The MapR-DB format is not included in the Apache Drill release. Drill includes a [maprdb format](https://maprdocs.mapr.com/home/Drill/UsingMapRdbFormat.html) for MapR-DB that is defined within the default dfs storage plugin configuration when you install Drill from the mapr-drill package on a MapR node. 

The maprdb format improves the estimated number of rows that Drill uses to plan a query. It also enables you to query tables like you would query files in a file system because MapR-DB and MapR-FS share the same namespace.

You can query tables stored across multiple directories. You do not need to create a table mapping to a directory before you query a table in the directory. You can select from any table in any directory the same way you would select from files in MapR-FS, using the same syntax.

Instead of including the name of a file, you include the table name in the query. The userid running the query must have read permission to access the MapR table.

**Example**  

       SELECT * FROM mfs.`/users/max/mytable`;   

Starting in Drill 1.14, the MapR Drill installation package includes a hive-maprdb-json-handler, which enables you to create Hive external tables from MapR-DB JSON tables and then query the tables using the Hive schema. Drill can use the native Drill reader to read the Hive external tables. The native Drill reader enables Drill to perform faster reads of data and apply filter pushdown optimizations. The hive-maprdb-json-handler is not included in the Apache Drill installation package.  

Starting in Drill 1.16, you can include the `readTimestampWithZoneOffset` option in the maprdb format plugin configuration. When enabled (set to 'true'), Drill converts timestamp values from UTC to local time zone when reading the values from MapR Database. The option is disabled by default and does not impact the `store.hive.maprdb_json.read_timestamp_with_timezone_offset` setting.  


