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

Drill uses ZooKeeper to
store persistent configuration data. The ZooKeeper PStore provider stores all
of the persistent configuration data in ZooKeeper except for query profile
data. The ZooKeeper PStore provider offloads query profile data to the Drill log directory on Drill nodes. 

ZooKeeper is the default for persistent configuration storage in Apache Drill, so it will be used as long as you do not specify another persistent store implementation.

However, if you are running multiple drillbits on different hosts you will probably want to change the location that query profiles are stored so they are accessible from all drillbits.

### Changing the Storage Location for Query Profiles

By default, query profiles are written to a folder on the local disk of the drillbit.  When running multiple drillbits, you should change the configuration so that the query profiles can be accessed by all the drillbits.  If you do not, then you can only access a query profile from the particular drillbit that created it.  This may result in query profiles being inaccessible in the Web UI and links to running queries being a broken link.

To change the location of these files, set the `sys.store.provider.zk.blobroot` property in the `drill.exec`
block in `<drill_installation_directory>/conf/drill-override.conf` on each
Drill node and then restart the Drillbit service.

Currently only HDFS paths are supported for this purpose.

**Example**

	drill.exec: {
	  cluster-id: "my_cluster_com-drillbits",
	  zk.connect: "<zkhostname>:<port>",
	  sys.store.provider.zk.blobroot: "hdfs:///apps/drill/pstore/"
	}

[Restart the Drillbit]({{site.baseurl}}/docs/starting-drill-in-distributed-mode/).

## Configuring HBase for Persistent Configuration Storage

To change the persistent storage mode for Drill, add or modify the
`sys.store.provider` block in `<drill_installation_directory>/conf/drill-
override.conf`.

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

## Using MongoDB for Persistent Configuration Storage

Some environments may prefer to use MongoDB for configuration storage because it supports password authentication and TLS, whereas ZooKeeper is typically deployed without these security features enabled.  To use MongoDB for this, add or modify the
`sys.store.provider` block in `<drill_installation_directory>/conf/drill-override.conf`, using the example below as a template.  The connection URL should look very familiar for users of MongoDB with the addition of the collection name to the database name using a ".".

Remember to give drill full access to the database you are storing this information in, or it will fail to start up.

**Example**

	sys.store.provider: {
	  class: "org.apache.drill.exec.store.mongo.config.MongoPersistentStoreProvider",
	  mongo: {
	    url: "mongodb://user:password@host/database.collection?ssl=true&authSource=database&authMechanism=SCRAM-SHA-1"	      
	  }
	},  

## Storing Query Profiles in Memory

As of Drill 1.11, Drill can store query profiles in memory instead of writing them to disk. For sub-second queries, writing the query profile to disk is expensive due to the interactions with the file system. You can enable the `drill.exec.profiles.store.inmemory` option in the drill-override.conf file if you want Drill to store the profiles for sub-second queries in memory instead of writing the profiles to disk. When you enable this option, Drill stores the profiles in memory for as long as the drillbit runs. When the drillbit restarts, the profiles no longer exist. You can set the maximum number of most recent profiles to retain in memory through the `drill.exec.profiles.store.capacity` option. See [Start-Up Options]({{site.baseurl}}/docs/start-up-options/) for more information. You must [restart the drillbit]({{site.baseurl}}/docs/starting-drill-in-distributed-mode/) after you enable the `drill.exec.profiles.store.inmemory` option. 


