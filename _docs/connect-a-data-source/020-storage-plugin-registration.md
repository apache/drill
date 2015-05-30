---
title: "Storage Plugin Registration"
parent: "Connect a Data Source"
---
You connect Drill to a file system, Hive, HBase, or other data source using storage plugins. Drill includes a number of storage plugins in the installation. On the Storage tab of the Web UI, you can view, create, reconfigure, and register a storage plugin. To open the Storage tab, go to `http://<IP address>:8047/storage`, where IP address is any one of the installed drill bits:

![drill-installed plugins]({{ site.baseurl }}/docs/img/plugin-default.png)

The Drill installation registers the `cp`, `dfs`, `hbase`, `hive`, and `mongo` storage plugins instances by default.

* `cp`  
  Points to a JAR file in the Drill classpath that contains the Transaction Processing Performance Council (TPC) benchmark schema TPC-H that you can query. 
* `dfs`  
  Points to the local file system on your machine, but you can configure this instance to
point to any distributed file system, such as a Hadoop or S3 file system. 
* `hbase`  
   Provides a connection to HBase/M7.
* `hive`  
   Integrates Drill with the Hive metadata abstraction of files, HBase/M7, and libraries to read data and operate on SerDes and UDFs.
* `mongo`  
   Provides a connection to MongoDB data.

In the Drill sandbox,  the `dfs` storage plugin connects you to the MapR File System (MFS). Using an installation of Drill instead of the sandbox, `dfs` connects you to the root of your file system.

