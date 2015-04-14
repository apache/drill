---
title: "MapR-DB Format"
parent: "Connect a Data Source"
---
Drill includes a `maprdb` format plugin for accessing data stored in MapR-DB. The Drill Sandbox also includes the following `maprdb` format plugin on a MapR node:

    {
      "type": "hbase",
      "config": {
        "hbase.table.namespace.mappings": "*:/tables"
      },
      "size.calculator.enabled": false,
      "enabled": true
    }

Using the Sandbox and this `maprdb` format plugin, you can query HBase tables located in the `/tables` directory, as shown in the ["Query HBase"](/docs/querying-hbase) examples.

The `dfs` storage plugin includes the maprdb format when you install Drill from the `mapr-drill` package on a MapR node. Click **Update** next to the `dfs` instance
in the Web UI of the Drill Sandbox to view the configuration for the `dfs` instance:

![drill query flow]({{ site.baseurl }}/docs/img/18.png)


The examples of the [CONVERT_TO/FROM functions](/docs/data-type-conversion#convert_to-and-convert_from) show how to adapt the `dfs` storage plugin to use the `maprdb` format plugin to query HBase tables on the Sandbox.

You modify the `dfs` storage plugin to create a table mapping to a directory in the MapR-FS file system. You then select the table by name.

**Example**

    SELECT * FROM myplugin.`mytable`;

The `maprdb` format plugin improves the
estimated number of rows that Drill uses to plan a query. Using the `dfs` storage plugin, you can query HBase and MapR-DB tables as you would query files in a file system. MapR-DB, MapR-FS, and Hadoop files share the same namespace.

