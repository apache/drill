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

* [Memory]({{site.baseurl}}/docs/configuring-resources-in-a-cluster)  
* [CPU]({{site.baseurl}}/docs/configuring-drill-in-a-cluster#how-to-manage-drill-cpu-resources)  
* Disk  

["How to Run Drill in a Cluster"]({{site.baseurl}}/docs/how-to-run-drill-in-a-cluster) covers configuration for sharing a drillbit and ["How Multiple Users Share a Drillbit"]({{site.baseurl}}/docs/how-multiple-users-share-a-drillbit) covers configuration for Drillbits running on different nodes in the cluster.