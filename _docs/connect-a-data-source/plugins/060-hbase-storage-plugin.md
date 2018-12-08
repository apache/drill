---
title: "HBase Storage Plugin"
date: 2018-12-08
parent: "Connect a Data Source"
---
When connecting Drill to an HBase data source using the HBase storage plugin installed with Drill, you need to specify a ZooKeeper quorum. Drill supports HBase version 1.x.

To view or change the HBase storage plugin configuration, use the [Drill Web UI]({{ site.baseurl }}/docs/plugin-configuration-basics/#using-the-drill-web-console). In the Web UI, select the **Storage** tab, and then click the **Update** button for the `hbase` storage plugin configuration. The following example shows a typical HBase storage plugin:

            {
              "type": "hbase",
              "config": {
                "hbase.zookeeper.quorum": "10.10.100.62,10.10.10.52,10.10.10.53",
                "hbase.zookeeper.property.clientPort": "2181"
              },
              "size.calculator.enabled": false,
              "enabled": true
            }

