---
title: "HBase Storage Plugin"
parent: "Storage Plugin Configuration"
---
Specify a ZooKeeper quorum to connect
Drill to an HBase data source. Drill supports HBase version 0.98.

To HBase storage plugin configuration installed with Drill appears as follows when you navigate to [http://localhost:8047](http://localhost:8047/), and select the **Storage** tab.

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

