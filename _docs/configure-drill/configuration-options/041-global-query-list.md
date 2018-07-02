---
title: "Global Query List"
date: 2018-07-02 01:09:32 UTC
parent: "Configuration Options"
---

A query profile is a summary of metrics collected for each query that Drill executes. Query profiles provide important information that you can use to monitor and analyze query performance. You can view query profiles in the Drill Web UI at `http(s)://<drill-hostname>:8047`.  

By default, you can only see the queries coordinated by the Drill node from which you access the Drill Web UI. For example, if you access the Drill Web UI at `http://10.10.20.56:8047`, you can only see the queries coordinated by Drill running on node 10.10.20.56. A Drill node that receives a query, coordinates the query and serves as the [Foreman]({{site.baseurl}}/docs/drill-query-execution/) for that query. Each Drill node can act as a Foreman for queries. If ZooKeeper or Load Balancer is distributing queries to multiple Drill nodes, query profiles are spread across all Drill nodes and you cannot see a global timeline of the queries when accessing the Web UI from one Drill node. 

To see a global query list (view of query profiles coordinated by all Drill nodes in one Web UI), you must configure the ZooKeeper [PStore (persistent configuration storage)]({{site.baseurl}}/docs/persistent-configuration-storage/) to point to a location on the distributed file system.  

**Note:** On MapR-FS, the query profiles are written to the distributed file system by default. You do not need to configure the ZooKeeper PStore.  

## Query Profile Storage Location   

Drill uses ZooKeeper to store persistent configuration data. The ZooKeeper PStore provider stores all of the persistent configuration data in ZooKeeper, except for query profile data. The ZooKeeper PStore provider offloads query profile data to the Drill log directory on each Drill node, for example `<drill_installation_directory>/logs/profiles`. 
 
Configure the ZooKeeper PStore location when you have Drill running on multiple nodes in a cluster, and you want to monitor query profiles for all of the queries in the Drill Web UI. Configuring a ZooKeeper PStore is also beneficial when space is limited on the local file system; you do not have to purge or archive the query profiles frequently due to the limited space.  

## Configuring the ZooKeeper PStore Location   

To configure the PStore, set the `sys.store.provider.zk.blobroot` property in the `drill.exec` block of the `<drill_installation_directory>/conf/drill-override.conf` file on each Drill node and then [restart the Drillbit service]({{site.baseurl}}/docs/starting-drill-in-distributed-mode/). 

The following example shows the configuration in `drill-override.conf` when you set the `/apps/drill/pstore/` directory in the Hadoop distributed file system as the ZooKeeper PStore location:  

       drill.exec: {
        cluster-id: "my_cluster_com-drillbits",
        zk.connect: "<zkhostname>:<port>",
        sys.store.provider.zk.blobroot: "hdfs:///apps/drill/pstore/"
       }
  


