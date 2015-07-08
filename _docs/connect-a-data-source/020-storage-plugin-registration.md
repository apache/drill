---
title: "Storage Plugin Registration"
parent: "Connect a Data Source"
---
You connect Drill to a file system, Hive, HBase, or other data source through a storage plugin. On the Storage tab of the Web UI, you can view and reconfigure a storage plugin. You can create a new name for the reconfigured version, thereby registering the new version. To open the Storage tab, go to `http://<IP address>:8047/storage`, where IP address is any one of the installed Drillbits in a distributed system or `localhost` in an embedded system:

![drill-installed plugins]({{ site.baseurl }}/docs/img/plugin-default.png)

The Drill installation registers the the `cp`, `dfs`, `hbase`, `hive`, and `mongo` storage plugin configurations.

* `cp`  
  Points to a JAR file in the Drill classpath that contains the Transaction Processing Performance Council (TPC) benchmark schema TPC-H that you can query. 
* `dfs`  
  Points to the local file system, but you can configure this storage plugin to
point to any distributed file system, such as a Hadoop or S3 file system. 
* `hbase`  
   Provides a connection to HBase.
* `hive`  
   Integrates Drill with the Hive metadata abstraction of files, HBase, and libraries to read data and operate on SerDes and UDFs.
* `mongo`  
   Provides a connection to MongoDB data.

In the [Drill sandbox]({{site.baseurl}}/docs/about-the-mapr-sandbox/), the `dfs` storage plugin connects you to a simulation of a distributed file system. If you install Drill, `dfs` connects you to the root of your file system.

Drill saves storage plugin configurations in a temporary directory (embedded mode) or in ZooKeeper (distributed mode). The storage plugin configuration persists after upgrading, so a configuration that you created in one version of Drill appears in the Drill Web UI of an upgraded version of Drill. For example, on Mac OS X, Drill uses `/tmp/drill/sys.storage_plugins` to store storage plugin configurations. To revert to the default storage plugins for a particular version, in embedded mode, delete the contents of this directory and restart the Drill shell.

