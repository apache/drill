---
title: "HBase Storage Plugin"
parent: "Storage Plugin Configuration"
---
[Previous](/docs/file-system-storage-plugin)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Back to Table of Contents](/docs)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Next](/docs/hive-storage-plugin)

Register a storage plugin instance and specify a zookeeper quorum to connect
Drill to an HBase data source. When you register a storage plugin instance for
an HBase data source, provide a unique name for the instance, and identify the
type as “hbase” in the Drill Web UI.

Drill supports HBase version 0.98.

To register HBase with Drill, complete the following steps:

  1. Navigate to [http://localhost:8047](http://localhost:8047/), and select the **Storage** tab
  2. In the disabled storage plugins section, click **Update** next to the `hbase` instance.
  3. In the Configuration window, specify the Zookeeper quorum and port. 
  
     **Example**
  
        {
          "type": "hbase",
          "config": {
            "hbase.zookeeper.quorum": "<zk1host,zk2host,zk3host> or <localhost>",
            "hbase.zookeeper.property.clientPort": "2181"
          },
          "enabled": false
        }

  4. Click **Enable**.

After you configure a storage plugin instance for the HBase, you can
issue Drill queries against it.

In the Drill sandbox, use the `dfs` storage plugin and the [MapR-DB format](/docs/mapr-db-format/) to query HBase files because the sandbox does not include HBase services.