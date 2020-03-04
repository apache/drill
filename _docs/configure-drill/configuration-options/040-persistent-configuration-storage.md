---
title: "Persistent Configuration Storage"
date: 2018-12-08
parent: "Configuration Options"
---
Drill stores persistent configuration data in a persistent configuration store
(PStore). The data stored in a PStore includes state information for storage plugins, query profiles, and ALTER SYSTEM settings. This data is encoded in JSON or Protobuf format. Drill can write this data to the local file system or a distributed file system, such as HDFS. As of Drill 1.11, Drill can [store query profiles in memory](https://drill.apache.org/docs/persistent-configuration-storage/#storing-query-profiles-in-memory) instead of writing the profiles to disk.

The default type of PStore configured depends on the Drill installation mode. The following table provides the persistent storage mode for each of the Drill
modes:

| Mode        | Description                                                                                                                                                                          |
|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Embedded    | Drill stores persistent data in the local file system. You cannot modify the PStore location for Drill in embedded mode.                                                             |
| Distributed | Drill stores persistent data in ZooKeeper, by default. You can modify where ZooKeeper offloads data, or you can change the persistent storage mode to HBase, for example.            |
  
{% include startnote.html %}Switching between storage modes does not migrate configuration data.{% include endnote.html %}

## Configuring ZooKeeper PStore

Drill uses ZooKeeper to store persistent configuration data. The ZooKeeper PStore provider stores all of the persistent configuration data in ZooKeeper except for query profile data. By default, Drill stores query profile data to the Drill log directory on Drill nodes. 

If you are running multiple drillbits, it is likely that the log folder is not shared between them. In this case each drillbit will only have access to profiles for the queries where it was the foreman. To make query profiles visible globally, configure drill to use a shared location for query profiles by setting the `drill.exec.sys.store.provider.zk.blobroot` in `drill-override.conf`.

**Examples**

Store query profiles on HDFS:

	drill.exec: {
	  sys.store.provider.zk.blobroot: "hdfs:///apps/drill/pstore/"
	}

Store query profiles on Amazon S3:

	drill.exec: {
	  sys.store.provider.zk.blobroot: "s3a://drill-query-profiles"
	}

Changes will take effect when you [Restart the Drillbit]({{site.baseurl}}/docs/starting-drill-in-distributed-mode/).

## Configuring HBase for Persistent Configuration Storage

To change the persistent storage mode for Drill, add or modify the
`sys.store.provider` block in `<drill_installation_directory>/conf/drill-
override.conf.`

**Example**

	sys.store.provider: {
	    class: "org.apache.drill.exec.store.hbase.config.HBasePStoreProvider",
	    hbase: {
	      table : "drill_store",
	      config: {
	      "hbase.zookeeper.quorum": "<ip_address>,<ip_address>,<ip_address >,<ip_address>",
	      "hbase.zookeeper.property.clientPort": "2181"
	      }
	    }
	  },  

## Storing Query Profiles in Memory

As of Drill 1.11, Drill can store query profiles in memory instead of writing them to disk. For sub-second queries, writing the query profile to disk is expensive due to the interactions with the file system. You can enable the `drill.exec.profiles.store.inmemory` option in the drill-override.conf file if you want Drill to store the profiles for sub-second queries in memory instead of writing the profiles to disk. When you enable this option, Drill stores the profiles in memory for as long as the drillbit runs. When the drillbit restarts, the profiles no longer exist. You can set the maximum number of most recent profiles to retain in memory through the `drill.exec.profiles.store.capacity` option. See [Start-Up Options]({{site.baseurl}}/docs/start-up-options/) for more information. You must [restart the drillbit]({{site.baseurl}}/docs/starting-drill-in-distributed-mode/) after you enable the `drill.exec.profiles.store.inmemory` option. 
