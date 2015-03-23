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

    SELECT * FROM sys.options WHERE type='BOOT'

## Configuring Start-Up Options

You can configure start-up options for each Drillbit in the `drill-
override.conf` file located in Drill’s` /conf` directory.

You may want to configure the following start-up options that control certain
behaviors in Drill:

<table ><tbody><tr><th >Option</th><th >Default Value</th><th >Description</th></tr><tr><td valign="top" >drill.exec.sys.store.provider</td><td valign="top" >ZooKeeper</td><td valign="top" >Defines the persistent storage (PStore) provider. The PStore holds configuration and profile data. For more information about PStores, see <a href="/docs/persistent-configuration-storage" rel="nofollow">Persistent Configuration Storage</a>.</td></tr><tr><td valign="top" >drill.exec.buffer.size</td><td valign="top" > </td><td valign="top" >Defines the amount of memory available, in terms of record batches, to hold data on the downstream side of an operation. Drill pushes data downstream as quickly as possible to make data immediately available. This requires Drill to use memory to hold the data pending operations. When data on a downstream operation is required, that data is immediately available so Drill does not have to go over the network to process it. Providing more memory to this option increases the speed at which Drill completes a query.</td></tr><tr><td valign="top" >drill.exec.sort.external.directoriesdrill.exec.sort.external.fs</td><td valign="top" > </td><td valign="top" >These options control spooling. The drill.exec.sort.external.directories option tells Drill which directory to use when spooling. The drill.exec.sort.external.fs option tells Drill which file system to use when spooling beyond memory files. <span style="line-height: 1.4285715;background-color: transparent;"> </span>Drill uses a spool and sort operation for beyond memory operations. The sorting operation is designed to spool to a Hadoop file system. The default Hadoop file system is a local file system in the /tmp directory. Spooling performance (both writing and reading back from it) is constrained by the file system. <span style="line-height: 1.4285715;background-color: transparent;"> </span>For MapR clusters, use MapReduce volumes or set up local volumes to use for spooling purposes. Volumes improve performance and stripe data across as many disks as possible.</td></tr><tr><td valign="top" colspan="1" >drill.exec.debug.error_on_leak</td><td valign="top" colspan="1" >True</td><td valign="top" colspan="1" >Determines how Drill behaves when memory leaks occur during a query. By default, this option is enabled so that queries fail when memory leaks occur. If you disable the option, Drill issues a warning when a memory leak occurs and completes the query.</td></tr><tr><td valign="top" colspan="1" >drill.exec.zk.connect</td><td valign="top" colspan="1" >localhost:2181</td><td valign="top" colspan="1" >Provides Drill with the ZooKeeper quorum to use to connect to data sources. Change this setting to point to the ZooKeeper quorum that you want Drill to use. You must configure this option on each Drillbit node.</td></tr><tr><td valign="top" colspan="1" >drill.exec.cluster-id</td><td valign="top" colspan="1" >my_drillbit_cluster</td><td valign="top" colspan="1" >Identifies the cluster that corresponds with the ZooKeeper quorum indicated. It also provides Drill with the name of the cluster used during UDP multicast. You must change the default cluster-id if there are multiple clusters on the same subnet. If you do not change the ID, the clusters will try to connect to each other to create one cluster.</td></tr></tbody></table></div>
