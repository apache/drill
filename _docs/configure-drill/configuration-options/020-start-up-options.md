---
title: "Start-Up Options"
date: 2018-07-06 21:03:51 UTC
parent: "Configuration Options"
---
The start-up options for Drill reside in a [HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md) configuration file format, which is a hybrid between a properties file and a JSON file. Drill start-up options consist of a group of files with a nested relationship. At the bottom of the file hierarchy are the default files that Drill provides, starting with `drill-default.conf`. 

The `drill-default.conf` file is overridden by one or more `drill-module.conf` files that Drill’s internal modules provide. The `drill-module.conf` files are overridden by the `drill-override.conf` file that you define.    

You can provide overrides on each drillbit using system properties of the form `-Dname=value` passed on the command line: 
 
    ./drillbit.sh start -Dname=value


You can see the following group of files throughout the source repository in
Drill:

	common/src/main/resources/drill-default.conf
	common/src/main/resources/drill-module.conf
	contrib/storage-hbase/src/main/resources/drill-module.conf
	contrib/storage-hive/core/src/main/resources/drill-module.conf
	contrib/storage-hive/hive-exec-shade/src/main/resources/drill-module.conf
	exec/java-exec/src/main/resources/drill-module.conf
	distribution/src/resources/drill-override.conf

These files are listed inside the associated JAR files in the Drill distribution tarball.

Each Drill module has a set of options that Drill incorporates. Drill’s
modular design enables you to create new storage plugins, set new operators,
or create UDFs. You can also include additional configuration options that you
can override as needed.

When you add a JAR file to Drill, you must include a `drill-module.conf` file
in the root directory of the JAR file that you add. The `drill-module.conf`
file tells Drill to scan that JAR file or associated object and include it.

## Viewing Start-Up Options

Run the following query to see a list of the available start-up options:

    SELECT * FROM sys.boot;

## Configuring Start-Up Options

You can configure start-up options for each drillbit in `<drill_home>/conf/drill-override.conf` .

The summary of start-up options, also known as boot options, lists default values. The following descriptions provide more detail on key options that are frequently reconfigured:

* **drill.exec.http.ssl_enabled**  
  Introduced in Drill 1.2. Enables or disables [HTTPS support]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/#https-support). Settings are TRUE and FALSE, respectively. The default is FALSE.  
  
* **drill.exec.sys.store.provider.class**  
  Defines the persistent storage (PStore) provider. The [PStore]({{site.baseurl}}/docs/persistent-configuration-storage) holds configuration and profile data.  
 
* **drill.exec.buffer.size**  
  Defines the amount of memory available, in terms of record batches, to hold data on the downstream side of an operation. Drill pushes data downstream as quickly as possible to make data immediately available. This requires Drill to use memory to hold the data pending operations. When data on a downstream operation is required, that data is immediately available so Drill does not have to go over the network to process it. Providing more memory to this option increases the speed at which Drill completes a query.  
  
* **drill.exec.spill.fs**  
Introduced in Drill 1.11. The default file system on the local machine into which the Sort, Hash Aggregate, and Hash Join operators spill data. This is the recommended option to use for spilling. You can configure this option so that data spills into a distributed file system, such as hdfs. For example, "hdfs:///". The default setting is "file:///". See [Sort-Based and Hash-Based Memory Constrained Operators]({{site.baseurl}}/docs/sort-based-and-hash-based-memory-constrained-operators/) for more information.   
  
* **drill.exec.spill.directories**  
Introduced in Drill 1.11. The list of directories into which the Sort, Hash Aggregate, and Hash Join operators spill data. The list must be an array with directories separated by a comma, for example ["/fs1/drill/spill" , "/fs2/drill/spill" , "/fs3/drill/spill"]. This is the recommended option for spilling to multiple directories. The default setting is ["/tmp/drill/spill"]. See [Sort-Based and Hash-Based Memory Constrained Operators]({{site.baseurl}}/docs/sort-based-and-hash-based-memory-constrained-operators/) for more information.  

* **drill.exec.zk.connect**  
  Provides Drill with the ZooKeeper quorum to use to connect to data sources. Change this setting to point to the ZooKeeper quorum that you want Drill to use. You must configure this option on each Drillbit node.  

* **drill.exec.profiles.store.inmemory**  
  Introduced in Drill 1.11. When set to TRUE, enables Drill to store query profiles in memory instead of writing the query profiles to disk. When set to FALSE, Drill writes the profile for each query to disk, which is either the local file system or a distributed file system, such as HDFS. For sub-second queries, writing the query profile to disk is expensive due to the interactions with the file system. Enable this option if you want Drill to store the profiles of sub-second queries in memory instead of writing them to disk. When you enable this option, Drill stores the profiles in memory for as long as the drillbit runs. When the drillbit restarts, the profiles no longer exist. You can set the maximum number of most recent profiles to retain in memory through the `drill.exec.profiles.store.capacity` option. Settings are TRUE and FALSE. Default is FALSE. See [Persistent Configuration Storage]({{site.baseurl}}/docs/persistent-configuration-storage/) for more information.  
 
* **drill.exec.profiles.store.capacity**  
  Introduced in Drill 1.11. Sets the maximum number of most recent profiles to retain in memory when the `drill.exec.profiles.store.inmemory` option is enabled. Default is 1000.  
  
* **drill.exec.storage.action\_on\_plugins\_override\_file**  
Introduced in Drill 1.14. Determines what happens to the [`storage-plugins-override.conf`]({{site.baseurl}}/docs/configuring-storage-plugins/configuring-storage-plugins-with-the-storage-plugins-override.conf-file) file after Drill successfully updates storage plugin configurations. If you do not want Drill to apply the configurations after restarting, set the option to `"rename"` or `"remove"`.  This option accepts the following values:  

       - `"none"` - (Default) The file remains in the directory after Drill uses the file.  
       - `"rename"` - The `storage-plugins-override.conf` file name is changed to `storage-plugins-override-[current_timestamp].conf` after Drill uses the file.  
       - `"remove"` - The file is removed after Drill uses the file for the first time.