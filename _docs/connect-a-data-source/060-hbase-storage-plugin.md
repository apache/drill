---
title: "HBase Storage Plugin"
parent: "Storage Plugin Configuration"
---
Register a storage plugin instance and specify a ZooKeeper quorum to connect
Drill to an HBase data source. When you register a storage plugin instance for
an HBase data source, provide a unique name for the instance, and identify the
type as “hbase” in the Drill Web UI.

Drill supports HBase version 0.98.

To register HBase with Drill, complete the following steps:

  1. Navigate to [http://localhost:8047](http://localhost:8047/), and select the **Storage** tab
  2. In the disabled storage plugins section, click **Update** next to the `hbase` instance.
  3. In the Configuration window, specify the ZooKeeper quorum and port. 
  

     **Example**  

            {
              "type": "hbase",
              "config": {
                "hbase.zookeeper.quorum": "10.10.100.62,10.10.10.52,10.10.10.53",
                "hbase.zookeeper.property.clientPort": "2181"
              },
              "size.calculator.enabled": false,
              "enabled": true
            }

  4. Click **Enable**.

After you configure a storage plugin instance for the HBase, you can
issue Drill queries against it.
