---
title: "Start-Up Options"
parent: "Configuration Options"
---
Drill’s start-up options reside in a HOCON configuration file format, which is
a hybrid between a properties file and a JSON file. Drill start-up options
consist of a group of files with a nested relationship. At the core of the
file hierarchy is `drill-default.conf`. This file is overridden by one or more
`drill-module.conf` files, which are overridden by the `drill-override.conf`
file that you define.

You can see the following group of files throughout the source repository in
Drill:

	common/src/main/resources/drill-default.conf
	common/src/main/resources/drill-module.conf
	contrib/storage-hbase/src/main/resources/drill-module.conf
	contrib/storage-hive/core/src/main/resources/drill-module.conf
	contrib/storage-hive/hive-exec-shade/src/main/resources/drill-module.conf
	exec/java-exec/src/main/resources/drill-module.conf
	distribution/src/resources/drill-override.conf

These files are listed inside the associated JAR files in the Drill
distribution tarball.

Each Drill module has a set of options that Drill incorporates. Drill’s
modular design enables you to create new storage plugins, set new operators,
or create UDFs. You can also include additional configuration options that you
can override as necessary.

When you add a JAR file to Drill, you must include a `drill-module.conf` file
in the root directory of the JAR file that you add. The `drill-module.conf`
file tells Drill to scan that JAR file or associated object and include it.

## Viewing Startup Options

You can run the following query to see a list of Drill’s startup options:

    SELECT * FROM sys.options WHERE type='BOOT';

## Configuring Start-Up Options

You can configure start-up options for each Drillbit in the `drill-
override.conf` file located in Drill’s` /conf` directory.

The summary of start-up options, also known as boot options, lists default values. The following descriptions provide more detail on key options that are frequently reconfigured:

* drill.exec.sys.store.provider.class  
  
  Defines the persistent storage (PStore) provider. The [PStore]({{ site.baseurl }}/docs/persistent-configuration-storage) holds configuration and profile data. 

* drill.exec.buffer.size

  Defines the amount of memory available, in terms of record batches, to hold data on the downstream side of an operation. Drill pushes data downstream as quickly as possible to make data immediately available. This requires Drill to use memory to hold the data pending operations. When data on a downstream operation is required, that data is immediately available so Drill does not have to go over the network to process it. Providing more memory to this option increases the speed at which Drill completes a query.

* drill.exec.sort.external.spill.directories

  Tells Drill which directory to use when spooling. Drill uses a spool and sort operation for beyond memory operations. The sorting operation is designed to spool to a Hadoop file system. The default Hadoop file system is a local file system in the /tmp directory. Spooling performance (both writing and reading back from it) is constrained by the file system. For MapR clusters, use MapReduce volumes or set up local volumes to use for spooling purposes. Volumes improve performance and stripe data across as many disks as possible.


* drill.exec.zk.connect  
  Provides Drill with the ZooKeeper quorum to use to connect to data sources. Change this setting to point to the ZooKeeper quorum that you want Drill to use. You must configure this option on each Drillbit node.

