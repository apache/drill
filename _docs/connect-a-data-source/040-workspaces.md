---
title: "Workspaces"
parent: "Storage Plugin Configuration"
---
When you register an instance of a file system data source, you can configure
one or more workspaces for the instance. A workspace is a directory within the
file system that you define. Drill searches the workspace to locate data when
you run a query.

Each workspace that you register defines a schema that you can connect to and
query. Configuring workspaces is useful when you want to run multiple queries
on files or tables in a specific directory. You cannot create workspaces for
`hive` and `hbase` instances, though Hive databases show up as workspaces in
Drill.

The following example shows an instance of a file type storage plugin with a
workspace named `json` configured to point Drill to the
`/users/max/drill/json/` directory in the local file system `(dfs)`:

    {
      "type" : "file",
      "enabled" : true,
      "connection" : "file:///",
      "workspaces" : {
        "json" : {
          "location" : "/users/max/drill/json/",
          "writable" : false,
          "defaultinputformat" : json
       } 
    },

{% include startnote.html %}The `connection` parameter in the configuration above is "`file:///`", connecting Drill to the local file system (`dfs`).{% include endnote.html %}
To connect to a Hadoop or MapR file system the `connection` parameter would be "`hdfs:///" `or` "maprfs:///", `respectively.

To query a file in the example `json` workspace, you can issue the `USE`
command to tell Drill to use the `json` workspace configured in the `dfs`
instance for each query that you issue:

**Example**

    USE dfs.json;
    SELECT * FROM dfs.json.`donuts.json` WHERE type='frosted'

If the `json` workspace did not exist, the query would have to include the
full path to the `donuts.json` file:

    SELECT * FROM dfs.`/users/max/drill/json/donuts.json` WHERE type='frosted';

Using a workspace alleviates the need to repeatedly enter the directory path
in subsequent queries on the directory.

### Default Workspaces

Each `file` and `hive` instance includes a `default` workspace. The `default`
workspace points to the file system or to the Hive metastore. When you query
files and tables in the `file` or `hive default` workspaces, you can omit the
workspace name from the query.

For example, you can issue a query on a Hive table in the `default workspace`
using either of the following formats and get the the same results:

**Example**

    SELECT * FROM hive.customers LIMIT 10;
    SELECT * FROM hive.`default`.customers LIMIT 10;

{% include startnote.html %}Default is a reserved word. You must enclose reserved words in back ticks.{% include endnote.html %}


Because HBase instances do not have workspaces, you can use the following
format to query a table in HBase:

    SELECT * FROM hbase.customers LIMIT 10;

After you register a data source as a storage plugin instance with Drill, and
optionally configure workspaces, you can query the data source.