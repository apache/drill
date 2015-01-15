---
title: "Registering HBase"
parent: "Workspaces"
---
Register a storage plugin instance and specify a zookeeper quorum to connect
Drill to an HBase data source. When you register a storage plugin instance for
an HBase data source, provide a unique name for the instance, and identify the
type as “hbase” in the Drill Web UI.

Currently, Drill only works with HBase version 0.94.

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

Once you have configured a storage plugin instance for the HBase, you can
issue Drill queries against it. For information about querying an HBase data
source, refer to [Querying
HBase](https://cwiki.apache.org/confluence/display/DRILL/Querying+HBase).