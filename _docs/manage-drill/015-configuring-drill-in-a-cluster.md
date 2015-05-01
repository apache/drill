---
title: "Configuring Drill in a Cluster"
parent: "Configuring Different Workloads"
---
Drill operations are memory and CPU-intensive. You need to statically partition the cluster to designate which partition handles which workload. 

To reserve memory resources for Drill, you modify the `warden.drill-bits.conf` file in `/opt/mapr/conf/conf.d`. This file is created automatically when you install Drill on a node. 

    [root@centos23 conf.d]# pwd
    /opt/mapr/conf/conf.d
    [root@centos23 conf.d]# ls
    warden.drill-bits.conf  warden.nodemanager.conf  warden.resourcemanager.conf

You add the following lines to the file:

    service.heapsize.min=<some value in MB>
    service.heapsize.max=<some value in MB>
    service.heapsize.percent=<some whole number value>

## Configuring Drill in a YARN-enabled MapR Cluster

For example, you have 120G of available memory that you allocate to following workloads in a Yarn-enabled cluster:

File system = 20G  
HBase = 20G  
Yarn = 20G  
OS = 8G  

To add Drill to the cluster, how do you change memory allocation? It depends on your application. If Yarn does most of the work, give Drill 20G, for example, and give Yarn 60G. If you expect a heavy query load, give Drill 60G and Drill 20G.

{% include startnote.html %}Drill will execute queries within Yarn soon.{% include endnote.html %} [DRILL-142](https://issues.apache.org/jira/browse/DRILL-142)

<!-- YARN consists of 2 main services ResourceManager(one instance in cluster, more if HA is configured) and NodeManager(one instance per node). See mr1.memory.percent, mr1.cpu.percent and 
mr1.disk.percent in warden.conf. Rest is given to YARN applications.
Also see /opt/mapr/conf/conf.d/warden.resourcemanager.conf and
 /opt/mapr/conf/conf.d/warden.nodemanager.conf for resources given to ResourceManager and NodeManager respectively.

## Configuring Drill in a MapReduce V1-enabled cluster

Similar files exist for other installed services, as described in [MapR documentation](http://doc.mapr.com/display/MapR/warden.%3Cservicename%3E.conf). For example:
## What are the memory/resource configurations set in warden in a non-YARN cluster? 

Every service will have 3 values defined in warden.conf (/opt/mapr/conf)
service.command.<servicename>.heapsize.percent
service.command.<servicename>.heapsize.max
service.command.<servicename>.heapsize.min
This is percentage of memory for that service bounded by min and max values.

There will also be additional files in /opt/mapr/conf/conf.d in format 
warden.<servicename>.conf. They will have entries like
service.heapsize.min
service.heapsize.max
service.heapsize.percent -->

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