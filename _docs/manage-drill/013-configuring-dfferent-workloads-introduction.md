---
title: "Configuring Different Workloads Introduction"
parent: "Configuring Different Workloads"
---

Drill supports multiple users sharing a Drillbit. You can also run separate Drillbits running on different nodes in the cluster.

Drill typically runs along side other workloads, including the following:  

* Mapreduce  
* Yarn  
* HBase  
* Hive and Pig  
* Spark  

You need to plan and configure these resources for use with Drill and other workloads: 

* [Memory]({{site.baseurl}}/docs/configuring-resources-in-a-mixed-cluster)  
* [CPU]({{site.baseurl}}/docs/configuring-drill-in-a-cluster#how-to-manage-drill-cpu-resources)  
* Disk  

Configure, memory, queues, and parallelization when users [share a Drillbit]({{site.baseurl}}/docs/configuring-resources-for-a-shared-drillbit).