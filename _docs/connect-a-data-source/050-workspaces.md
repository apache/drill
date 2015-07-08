---
title: "Workspaces"
parent: "Storage Plugin Configuration"
---
You can define one or more workspaces in a storage plugin configuration. The workspace defines the directory location of files in a local or distributed file system. Drill searches the workspace to locate data when
you run a query. The `default`
workspace points to the root of the file system. 

Configuring `workspaces` to include a file location simplifies the query, which is important when querying the same data source repeatedly. After you configure a long path name in the workspaces location property, instead of
using the full path to the data source, you use dot notation in the FROM
clause.

``<workspaces>.`<location>```

To query the data source while you are not *using* that storage plugin, include the plugin name. This syntax assumes you did not issue a USE statement to connect to a storage plugin that defines the
location of the data:

``<plugin>.<workspaces>.`<location>```


## No Workspaces for Hive and HBase

You cannot configure workspaces for
`hive` and `hbase`, though Hive databases show up as workspaces in
Drill. Each `hive` instance includes a `default` workspace that points to the  Hive metastore. When you query
files and tables in the `hive default` workspaces, you can omit the
workspace name from the query.

For example, you can issue a query on a Hive table in the `default workspace`
using either of the following queries and get the same results:

**Example**

    SELECT * FROM hive.customers LIMIT 10;
    SELECT * FROM hive.`default`.customers LIMIT 10;

{% include startnote.html %}Default is a reserved word. You must enclose reserved words in back ticks.{% include endnote.html %}

Because the HBase storage plugin configuration does not have a workspace, you can use the following
query:

    SELECT * FROM hbase.customers LIMIT 10;

