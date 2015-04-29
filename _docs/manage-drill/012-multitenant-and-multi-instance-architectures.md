---
title: "Multitenant and Multi-instance Architectures"
parent: "Manage Drill"
---

Drill supports multitenant and multi-instance architectures. In a multitenant architecture, multiple users share a drillbit. In a multi-instance architectures, tenants use separate drillbits running on different nodes in the cluster.

Drill typically runs along side many application frameworks, including the following:  

* Mapreduce  
* Yarn  
* HBase  
* Hive and Pig  
* Spark  
* Sqoop  

You need to plan and configure the resources for use with Drill in a multitenant or multi-instance environment. Currently, you can configure the following resources:

* Memory  
* CPU  
* Disk  

["How to Run Drill in a Cluster"]({{site.baseurl}}/docs/how-to-run-drill-in-a-cluster) covers configuration for a multitenant environment and ["How Multiple Users Share a Drillbit"]({{site.baseurl}}/docs/how-multiple-users-share-a-drillbit) covers configuration for a multi-instance environment.