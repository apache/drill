---
title: "Configuring a Multitenant Cluster Introduction"
slug: "Configuring a Multitenant Cluster Introduction"
parent: "Configuring a Multitenant Cluster"
---

Drill supports multiple users sharing a Drillbit and running separate Drillbits on different nodes in the cluster. Drill typically runs along side other workloads, including the following:  

* Mapreduce  
* Yarn  
* HBase  
* Hive and Pig  
* Spark  

You need to plan and configure the following resources for use with Drill and other workloads: 

* [Memory]({{site.baseurl}}/docs/configuring-multitenant-resources)  
* [CPU]({{site.baseurl}}/docs/configuring-multitenant-resources/#how-to-manage-drill-cpu-resources)  
* [Disk]({{site.baseurl}}/docs/configuring-multitenant-resources/#how-to-manage-drill-disk-resources) 

When users share a Drillbit, [configure queues]({{site.baseurl}}/docs/configuring-resources-for-a-shared-drillbit/#configuring-query-queuing) and [parallelization]({{site.baseurl}}/docs/configuring-resources-for-a-shared-drillbit/#configuring-parallelization) in addition to memory. 
