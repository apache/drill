---
title: "Persistent Configuration Storage"
parent: "Configuration Options"
---
Drill stores persistent configuration data in a persistent configuration store
(PStore). This data is encoded in JSON or Protobuf format. Drill can use the
local file system or a distributed file system, such as HDFS or MapR-FS to store this data. The data
stored in a PStore includes state information for storage plugins, query
profiles, and ALTER SYSTEM settings. The default type of PStore configured
depends on the Drill installation mode.

The following table provides the persistent storage mode for each of the Drill
modes:

| Mode        | Description                                                                                                                                                             |
|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Embedded    | Drill stores persistent data in the local file system. You cannot modify the PStore location for Drill in embedded mode.                                                |
| Distributed | Drill stores persistent data in ZooKeeper, by default. You can modify where ZooKeeper offloads data, or you can change the persistent storage mode to HBase or MapR-DB. |
  
{% include startnote.html %}Switching between storage modes does not migrate configuration data.{% include endnote.html %}

## Configuring ZooKeeper PStore

Drill uses ZooKeeper to
store persistent configuration data. The ZooKeeper PStore provider stores all
of the persistent configuration data in ZooKeeper except for query profile
data. The ZooKeeper PStore provider offloads query profile data to the Drill log directory on Drill nodes. 

You need to configure the ZooKeeper PStore to use the Drill Web UI when running multiple Drillbits. 

### Why Configure the ZooKeeper PStore

When you run multiple DrillBits, configure a specific location for ZooKeeper to offload the query profile data instead of accepting the default temporary location. All Drillbits in the cluster cannot access the temporary location. Consequently, when you do not configure a location on the distributed file system, queries sent to some Drillbits do not appear in the Completed section of the Drill Web UI. Also, some Running links that you click to get information about running queries are broken links.

### How to Configure the ZooKeeper PStore

To configure the ZooKeeper PStore, set the `sys.store.provider.zk.blobroot` property in the `drill.exec`
block in `<drill_installation_directory>/conf/drill-override.conf` on each
Drill node and then restart the Drillbit service.

**Example**

	drill.exec: {
	 cluster-id: "my_cluster_com-drillbits",
	 zk.connect: "<zkhostname>:<port>",
	 sys.store.provider.zk.blobroot: "maprfs://<directory to store pstore data>/"
	}

Issue the following command to restart the Drillbit on all Drill nodes:

    maprcli node services -name drill-bits -action restart -nodes <node IP addresses separated by a space>

## HBase for Persistent Configuration Storage

To change the persistent storage mode for Drill, add or modify the
`sys.store.provider` block in `<drill_installation_directory>/conf/drill-
override.conf.`

**Example**

	sys.store.provider: {
	    class: "org.apache.drill.exec.store.hbase.config.HBasePStoreProvider",
	    hbase: {
	      table : "drill_store"
	      config: {
	      "hbase.zookeeper.quorum": "<ip_address>,<ip_address>,<ip_address >,<ip_address>",
	      "hbase.zookeeper.property.clientPort": "2181"
	      }
	    }
	  },

## MapR-DB for Persistent Configuration Storage

If you have MapR-DB in your cluster, you can use MapR-DB for persistent
configuration storage. Using MapR-DB to store persistent configuration data
can prevent memory strain on ZooKeeper in clusters running heavy workloads.

To change the persistent storage mode to MapR-DB, add or modify the
`sys.store.provider` block in `<drill_installation_directory>/conf/drill-
override.conf` on each Drill node and then restart the Drillbit service.

**Example**

	sys.store.provider: {
	class: "org.apache.drill.exec.store.hbase.config.HBasePStoreProvider",
	hbase: {
	  table : "/tables/drill_store",
	    }
	},

Issue the following command to restart the Drillbit on all Drill nodes:

    maprcli node services -name drill-bits -action restart -nodes <node IP addresses separated by a space>

