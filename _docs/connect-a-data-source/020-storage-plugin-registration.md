---
title: "Storage Plugin Registration"
date: 2018-02-13 00:00:43 UTC
parent: "Connect a Data Source"
---
You connect Drill to data sources, such as a file system, Hive, or HBase through storage plugins. You can see the default enabled and disabled storage plugins on the Storage page in the Drill Web Console. You can easily enable and disable storage plugins, reconfigure storage plugins, and create new storage plugin configurations.

 If [HTTPS support]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/#https-support) is not enabled (the default), go to `http://<IP address>:8047/storage` to view and configure a storage plugin. IP address is the host name or IP address of one of the installed Drillbits in a distributed system or `localhost` in an embedded system. If HTTPS support is enabled and you are [authorized]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/) to view and configure a storage plugin, go to `https://<IP address>:8047/storage`. 

The Drill installation registers the following storage plugin configurations, with the cp and dfs storage plugins enabled by default; you must enable the other storage plugins:

* `cp`  
   Points to JAR files in the Drill classpath, such as [`employee.json`]({{site.baseurl}}/docs/querying-json-files/) that you can query. 
* `dfs`  
  Points to the local file system, but you can configure this storage plugin to
point to any distributed file system, such as a Hadoop or S3 file system. 
* `hbase`  
   Provides a connection to HBase.
* `hive`  
   Integrates Drill with the Hive metadata abstraction of files, HBase, and libraries to read data and operate on SerDes and UDFs.
* `mongo`  
   Provides a connection to MongoDB data.
* `kafka`  
   Provides a connection to Kafka.
- `kudu`  
   Provides a connection to Kudu.
- `opentsdb`  
   Provides a connectin to OpenTSDB.


In the [Drill sandbox]({{site.baseurl}}/docs/about-the-mapr-sandbox/), the `dfs` storage plugin configuration connects you to a Hadoop environment pre-configured with Drill. If you install Drill, `dfs` connects you to the root of your file system.

## Registering a Storage Plugin Configuration

To register a new storage plugin configuration, enter a storage name, click **CREATE**, provide a configuration in JSON format, and click **UPDATE**. 

In Drill 1.2 and later, updating a storage plugin configuration and other storage plugin tasks require [authorization]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/) if security is enabled.

## Storage Plugin Configuration Persistence

Drill saves storage plugin configurations in a temporary directory (embedded mode) or in ZooKeeper (distributed mode). For example, on Mac OS X, Drill uses `/tmp/drill/sys.storage_plugins` to store storage plugin configurations. The temporary directory clears when you reboot. When you run drill in embedded mode, add the sys.store.provider.local.path option to the drill-override.conf file and the path for storing the plugin configurations. For example:

     drill.exec: {
     	cluster-id: "drillbits1",
     	zk.connect: "localhost:2181",
     	sys.store.provider.local.path="/mypath"
     }  

See [Persistent Configuration Storage]({{site.baseurl}}/docs/persistent-configuration-storage/) for more information.