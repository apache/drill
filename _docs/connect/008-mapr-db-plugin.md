---
title: "MapR-DB Plugin for Apache Drill"
parent: "Connect to Data Sources"
---
Drill includes a `maprdb` format plugin for MapR-DB that is defined within the
default `dfs` storage plugin instance when you install Drill from the `mapr-drill` package on a MapR node. The `maprdb` format plugin improves the
estimated number of rows that Drill uses to plan a query. It also enables you
to query tables like you would query files in a file system because MapR-DB
and MapR-FS share the same namespace.

You can query tables stored across multiple directories. You do not need to
create a table mapping to a directory before you query a table in the
directory. You can select from any table in any directory the same way you
would select from files in MapR-FS, using the same syntax.

Instead of including the name of a file, you include the table name in the
query.

**Example**

    SELECT * FROM mfs.`/users/max/mytable`;

Drill stores the `maprdb` format plugin in the `dfs` storage plugin instance,
which you can view in the Drill Web UI. You can access the Web UI at
[http://localhost:8047/storage](http://localhost:8047/storage). Click **Update** next to the `dfs` instance
in the Web UI to view the configuration for the `dfs` instance.

The following image shows a portion of the configuration with the `maprdb`
format plugin for the `dfs` instance:

![drill query flow]({{ site.baseurl }}/docs/img/18.png)
