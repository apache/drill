---
title: "Workspaces"
parent: "Storage Plugin Configuration"
---
When you register an instance of a file system data source, you can configure
one or more workspaces for the instance. The workspace defines the default directory location of files in a local or distributed file system. The `default`
workspace points to the root of the file system. Drill searches the workspace to locate data when
you run a query.

You cannot create workspaces for
`hive` and `hbase` storage plugins, though Hive databases show up as workspaces in
Drill. Each `hive` instance includes a `default` workspace that points to the  Hive metastore. When you query
files and tables in the `hive default` workspaces, you can omit the
workspace name from the query.

For example, you can issue a query on a Hive table in the `default workspace`
using either of the following formats and get the same results:

**Example**

    SELECT * FROM hive.customers LIMIT 10;
    SELECT * FROM hive.`default`.customers LIMIT 10;

{% include startnote.html %}Default is a reserved word. You must enclose reserved words in back ticks.{% include endnote.html %}

Because HBase instances do not have workspaces, you can use the following
format to query a table in HBase:

    SELECT * FROM hbase.customers LIMIT 10;

After you register a data source as a storage plugin instance with Drill, and
optionally configure workspaces, you can query the data source.

