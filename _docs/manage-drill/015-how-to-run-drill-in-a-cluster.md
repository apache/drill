---
title: "How to Run Drill in a Cluster"
parent: "Multitenant and Multi-instance Architectures"
---
Drill operations are memory and CPU-intensive. You need to statically partition the cluster to designation which partition handles which workload. For example, you have 120G of available memory that you allocate to following workloads in a Yarn-enabled cluster:

File system = 20G  
HBase = 20G  
Yarn = 20G  
OS = 8G  

To add Drill to the cluster, how do you change memory allocation? It depends on your application. If Yarn does most of the work, give Drill 20G, for example, and give Yarn 60G. If you expect a heavy query load, give Drill 60G and Drill 20G.

{% include startnote.html %}Drill will execute queries within Yarn soon.{% include endnote.html %}

For information about Drill and Yarn, see [DRILL-142](https://issues.apache.org/jira/browse/DRILL-142).

## Managing Memory

To run Drill in a cluster with MapReduce, HBase, Spark, and other workloads, manage memory according to your application needs. 

To run Drill in a MapR cluster, allocate memory by configuring settings in warden.conf, as described in the [MapR documentation]().

### Drill Memory
You can configure the amount of direct memory allocated to a Drillbit for
query processing, as described in the section, ["Configuring Memory"](({{site.baseurl}}/docs/configuring-memory).

### Memory in a MapR Cluster
Memory and disk for Drill and other services that are not associated with roles on a MapR cluster are shared with other services. You manage the chunk of memory for these services in os heap settings in `warden.conf` and in configuration files of the particular service. The warden os heap settings are:

    service.command.os.heapsize.percent
    service.command.os.heapsize.max
    service.command.os.heapsize.min

For more information about managing memory in a MapR cluster, see the following sections in the MapR documentation:
* [Memory Allocation for Nodes](http://doc.mapr.com/display/MapR40x/Memory+Allocation+for+Nodes)
* [Cluster Resource Allocation](http://doc.mapr.com/display/MapR40x/Cluster+Resource+Allocation)
* [Customizing Memory Settings for MapReduce v1](http://doc.mapr.com/display/MapR40x/Customize+Memory+Settings+for+MapReduce+v1)

## How to Manage Drill CPU Resources
Currently, you do not manage CPU resources within Drill. [Use Linux `cgroups`](http://en.wikipedia.org/wiki/Cgroups) to manage the CPU resources.