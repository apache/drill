---
title: "Controlling Parallelization to Balance Performance with Multi-Tenancy"
date: 2016-11-21 22:28:45 UTC
parent: "Query Plans and Tuning"
--- 

When you run Drill in a multi-tenant environment, (in conjunction with other workloads in a cluster, such as MapReduce) you may need to modify Drill settings and options to maximize performance, or reduce the allocated resources to other applications. See [Configuring Multi-Tenant Resources]({{ site.baseurl }}/docs/configuring-multitenant-resources/).
Drill is memory intensive and therefore requires sufficient memory to run optimally. You can modify how much memory that you want allocated to Drill. Drill typically performs better with as much memory as possible. See [Configuring Drill Memory]({{ site.baseurl }}/docs/configuring-drill-memory/).
 
Reducing the level of parallelism in Drill can also help to balance the workloads and avoid resource conflicts. See [Configuring Parallelization]({{ site.baseurl }}/docs/configuring-resources-for-a-shared-drillbit/#configuring-parallelization).

