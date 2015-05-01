---
title: "Configuring Resources in a Cluster"
parent: "Configuring Different Workloads"
---
Drill operations are memory and CPU-intensive. You need to statically partition the cluster to designate which partition handles which workload. 

## Configuring Drill Memory in a Cluster with other Workloads

To configure memory resources for Drill, you modify the `warden.drill-bits.conf` file in `/opt/mapr/conf/conf.d`. This file is created automatically when you install Drill on a node. 

    [root@centos23 conf.d]# pwd
    /opt/mapr/conf/conf.d
    [root@centos23 conf.d]# ls
    warden.drill-bits.conf  warden.nodemanager.conf  warden.resourcemanager.conf

You add the following lines to the wardens.drill-bits.conf file to configure memory resources for Drill:

    service.heapsize.min=<some value in MB>
    service.heapsize.max=<some value in MB>
    service.heapsize.percent=<a whole number>

This service.heapsize.percent is the percentage of memory for the service bounded by min and max values.

## Configuring Drill in a YARN-enabled MapR Cluster

To add Drill to a YARN-enabled cluster, you change memory resources depending on your application. For example, you have 120G of available memory that you allocate to following workloads in a Yarn-enabled cluster:

File system = 20G  
HBase = 20G  
Yarn = 20G  
OS = 8G  

If Yarn does most of the work, give Drill 20G, for example, and give Yarn 60G. If you expect a heavy query load, give Drill 60G and Drill 20G.

{% include startnote.html %}Drill will execute queries within Yarn soon.{% include endnote.html %} [DRILL-142](https://issues.apache.org/jira/browse/DRILL-142)

YARN consists of two main services:

* ResourceManager  
  There is at least one instance in a cluster, more if you configure high availability.  
* NodeManager  
  There is one instance per node. 

Configure ResourceManager and NodeManager memory in `/opt/mapr/conf/conf.d/warden.resourcemanager.conf` and
 `/opt/mapr/conf/conf.d/warden.nodemanager.conf`. Configure the following warden.nodemanager.conf settings. The defaults are:

    service.heapsize.min=64
    service.heapsize.max=325
    service.heapsize.percent=2

Memory allocation for NodeManager and ResourceManager is used only to calculate total memory required for all services to run. The -Xmx option is not set, allowing memory on to grow as needed. If you want to place an upper limit on memory set YARN_NODEMANAGER_HEAPSIZE or YARN_RESOURCEMANAGER_HEAPSIZE environment variable in /opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop/yarn-env.sh.

### MapReduce v1 Resources

To configure memory for MapReduce v1 resources set mr1.memory.percent, mr1.cpu.percent and mr1.disk.percent in warden.conf, as described in section ["Resource Allocation for Jobs and Applications"](http://doc.mapr.com/display/MapR/Resource+Allocation+for+Jobs+and+Applications) of the MapR documentation. Remaining memory is given to YARN applications. 


### MapReduce v2 and other Resources

Similar files exist for other installed services, as described in [MapR documentation](http://doc.mapr.com/display/MapR/warden.%3Cservicename%3E.conf). 

You configure memory for each service by setting three values in `warden.conf`.

    service.command.<servicename>.heapsize.percent
    service.command.<servicename>.heapsize.max
    service.command.<servicename>.heapsize.min

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