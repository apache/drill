---
title: "Multitenant and Multi-instance Architectures Introduction"
parent: "Multitenant and Multi-instance Architectures"
---

Drill supports multitenant and multi-instance architectures. In a multitenant architecture, multiple users share a drillbit. In a multi-instance architectures, tenants use separate drillbits running on different nodes in the cluster.

Drill typically runs along side many application frameworks, including the following:  

* Mapreduce  
* Yarn  
* HBase  
* Hive and Pig  
* Spark  

You need to plan and configure these resources for use with Drill in a multitenant or multi-instance environment: 

* [Memory]({{site.baseurl}}/docs/configuring-memory)  
* [CPU]({{site.baseurl}}/docs/how-to-manage-drill-cpu-resources)  
* Disk  

["How to Run Drill in a Cluster"]({{site.baseurl}}/docs/how-to-run-drill-in-a-cluster) covers configuration for a multitenant environment and ["How Multiple Users Share a Drillbit"]({{site.baseurl}}/docs/how-multiple-users-share-a-drillbit) covers configuration for a multi-instance environment.