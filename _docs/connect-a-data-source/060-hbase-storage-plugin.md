---
title: "HBase Storage Plugin"
parent: "Storage Plugin Configuration"
---
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
                "hbase.zookeeper.quorum": "10.10.100.62,10.10.10.52,10.10.10.53",
                "hbase.zookeeper.property.clientPort": "2181"
              },
              "size.calculator.enabled": false,
              "enabled": true
            }

  4. Click **Enable**.

The hbase.zookeeper.property.clientPort shown here and in the default hbase storage plugin is 2181. In a MapR cluster, the port is 5181; however, in a MapR cluster, use the maprdb storage plugin format instead of the hbase storage plugin. 

After you configure a storage plugin instance for the HBase, you can
issue Drill queries against it.

In the Drill sandbox, use the `dfs` storage plugin and the [MapR-DB format]({{ site.baseurl }}/docs/mapr-db-format/) to query HBase files because the sandbox does not include HBase services.