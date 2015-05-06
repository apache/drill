---
title: "Configuring a Multitenant Cluster Introduction"
parent: "Configuring a Multitenant Cluster"
---

Drill supports multiple users sharing a Drillbit. You can also run separate Drillbits running on different nodes in the cluster.

Drill typically runs along side other workloads, including the following:  

* Mapreduce  
* Yarn  
* HBase  
* Hive and Pig  
* Spark  

You need to plan and configure these resources for use with Drill and other workloads: 

* [Memory]({{site.baseurl}}/docs/configuring-multitenant-resources)  
* [CPU]({{site.baseurl}}/docs/configuring-multitenant-resources#how-to-manage-drill-cpu-resources)  
* Disk  

Configure, memory, queues, and parallelization when users [share a Drillbit]({{site.baseurl}}/docs/configuring-resources-for-a-shared-drillbit).