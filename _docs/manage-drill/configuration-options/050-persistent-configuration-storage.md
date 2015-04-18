---
title: "Persistent Configuration Storage"
parent: "Configuration Options"
---
Drill stores persistent configuration data in a persistent configuration store
(PStore). This data is encoded in JSON or Protobuf format. Drill can use the
local file system, ZooKeeper, HBase, or MapR-DB to store this data. The data
stored in a PStore includes state information for storage plugins, query
profiles, and ALTER SYSTEM settings. The default type of PStore configured
depends on the Drill installation mode.

The following table provides the persistent storage mode for each of the Drill
modes:

<table ><tbody><tr><th >Mode</th><th >Description</th></tr><tr><td valign="top" >Embedded</td><td valign="top" >Drill stores persistent data in the local file system. <br />You cannot modify the PStore location for Drill in embedded mode.</td></tr><tr><td valign="top" >Distributed</td><td valign="top" >Drill stores persistent data in ZooKeeper, by default. <br />You can modify where ZooKeeper offloads data, <br />or you can change the persistent storage mode to HBase or MapR-DB.</td></tr></tbody></table></div>
  
{% include startnote.html %}Switching between storage modes does not migrate configuration data.{% include endnote.html %}

## ZooKeeper for Persistent Configuration Storage

To make Drill installation and configuration simple, Drill uses ZooKeeper to
store persistent configuration data. The ZooKeeper PStore provider stores all
of the persistent configuration data in ZooKeeper except for query profile
data.

The ZooKeeper PStore provider offloads query profile data to the
${DRILL_LOG_DIR:-/var/log/drill} directory on Drill nodes. If you want the
query profile data stored in a specific location, you can configure where
ZooKeeper offloads the data.

To modify where the ZooKeeper PStore provider offloads query profile data,
configure the `sys.store.provider.zk.blobroot` property in the `drill.exec`
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
	      table : "drill_store",
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

