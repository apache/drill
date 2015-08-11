---
title: "Workspaces"
parent: "Storage Plugin Configuration"
---
You can define one or more workspaces in a storage plugin configuration. The workspace defines the location of files in subdirectories of a local or distributed file system. Drill searches the workspace to locate data when
you run a query. The `default`
workspace points to the root of the file system. 

Configuring workspaces to include a subdirectory simplifies the query, which is important when querying the same files repeatedly. After you configure a long path name in the workspace `location` property, instead of
using the full path name to the data source, you use dot notation in the FROM
clause.

``<workspace name>.`<location>```

Where `<location>` is the path name of a subdirectory, such as `/users/max/drill/json` enclosed in double quotation marks as shown in the ["Querying Donuts Example."](/docs/file-system-storage-plugin/#querying-donuts-example)

To query the data source when you have not set the default schema name to the storage plugin configuration, include the plugin name. This syntax assumes you did not issue a USE statement to connect to a storage plugin that defines the
location of the data:

``<plugin>.<workspace name>.`<location>```


## No Workspaces for Hive and HBase

You cannot include workspaces in the configurations of the
`hive` and `hbase` plugins installed with Apache Drill, though Hive databases show up as workspaces in
Drill. Each `hive` storage plugin configuration includes a `default` workspace that points to the  Hive metastore. When you query
files and tables in the `hive default` workspaces, you can omit the
workspace name from the query.

For example, you can issue a query on a Hive table in the `default workspace`
using either of the following queries and get the same results:

**Example**

    SELECT * FROM hive.customers LIMIT 10;
    SELECT * FROM hive.`default`.customers LIMIT 10;

{% include startnote.html %}Default is a reserved word. You must enclose reserved words when used as identifiers in back ticks.{% include endnote.html %}

Because the HBase storage plugin does not accommodate a workspace, you can use the following
query:

    SELECT * FROM hbase.customers LIMIT 10;

