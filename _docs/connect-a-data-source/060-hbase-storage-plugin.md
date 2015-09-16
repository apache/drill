---
title: "HBase Storage Plugin"
parent: "Storage Plugin Configuration"
---
When connecting Drill to an HBase data source using the HBase storage plugin installed with Drill, you need to specify a ZooKeeper quorum. Drill supports HBase version 0.98.

To view or change the HBase storage plugin configuration, use the [Drill Web Console]({{ site.baseurl }}/docs/plugin-configuration-basics/#using-the-drill-web-ui). In the Web Console, select the **Storage** tab, and then click the **Update** button for the `hbase` storage plugin configuration. The following example shows a typical HBase storage plugin:

            {
              "type": "hbase",
              "config": {
                "hbase.zookeeper.quorum": "10.10.100.62,10.10.10.52,10.10.10.53",
                "hbase.zookeeper.property.clientPort": "2181"
              },
              "size.calculator.enabled": false,
              "enabled": true
            }

