---
title: "Configuring Multitenant Resources"
parent: "Configuring a Multitenant Cluster"
---
Drill operations are memory and CPU-intensive. Currently, Drill resources are managed outside of any cluster management service, such as the MapR warden service. In a multitenant or any other type of cluster, YARN-enabled or not, you configure memory and memory usage limits for Drill by modifying drill-env.sh as described in ["Configuring Drill Memory"]({{site.baseurl}}/docs/configuring-drill-memory).

Configure a multitenant cluster to account for resources required for Drill. For example, on a MapR cluster, ensure warden accounts for resources required for Drill. Configuring `drill-env.sh` allocates resources for Drill to use during query execution, while configuring the following properties in `warden-drill-bits.conf` prevents warden from committing the resources to other processes.

    service.heapsize.min=<some value in MB>
    service.heapsize.max=<some value in MB>
    service.heapsize.percent=<a whole number>

{% include startimportant.html %}Set the `service.heapsize` properties in `warden.drill-bits.conf` regardless of whether you changed defaults in `drill-env.sh` or not.{% include endimportant.html %}

The section, ["Configuring Drill in a YARN-enabled MapR Cluster"]({{site.baseurl}}/docs/configuring-multitenant-resources#configuring-drill-in-a-yarn-enabled-mapr-cluster) shows an example of setting the `service.heapsize` properties. The `service.heapsize.percent` is the percentage of memory for the service bounded by minimum and maximum values. Typically, users change `service.heapsize.percent`. Using a percentage has the advantage of increasing or decreasing resources according to different node's configuration. For more information about the `service.heapsize` properties, see the section, ["warden.<servicename>.conf"](http://doc.mapr.com/display/MapR/warden.%3Cservicename%3E.conf).

You need to statically partition the cluster to designate which partition handles which workload. To configure resources for Drill in a MapR cluster, modify one or more of the following files in `/opt/mapr/conf/conf.d` that the installation process creates. 

* `warden.drill-bits.conf`
* `warden.nodemanager.conf`
* `warden.resourcemanager.conf`

Configure Drill memory by modifying `warden.drill-bits.conf` in YARN and non-YARN clusters. Configure other resources by modifying `warden.nodemanager.conf `and `warden.resourcemanager.conf `in a YARN-enabled cluster.

## Configuring Drill in a YARN-enabled MapR Cluster

To add Drill to a YARN-enabled cluster, change memory resources to suit your application. For example, you have 120G of available memory that you allocate to following workloads in a Yarn-enabled cluster:

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

ResourceManager and NodeManager memory in `warden.resourcemanager.conf` and
 `warden.nodemanager.conf` are set to the following defaults. 

    service.heapsize.min=64
    service.heapsize.max=325
    service.heapsize.percent=2

Change these settings for NodeManager and ResourceManager to reconfigure the total memory required for YARN services to run. If you want to place an upper limit on memory set YARN_NODEMANAGER_HEAPSIZE or YARN_RESOURCEMANAGER_HEAPSIZE environment variable in `/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop/yarn-env.sh`. The `-Xmx` option is not set, allowing memory on to grow as needed.

### MapReduce v1 Resources

The following default settings in `/opt/mapr/conf/warden.conf` control MapReduce v1 memory:

    mr1.memory.percent=50
    mr1.cpu.percent=50
    mr1.disk.percent=50

Modify these settings to reconfigure MapReduce v1 resources to suit your application needs, as described in section ["Resource Allocation for Jobs and Applications"](http://doc.mapr.com/display/MapR/Resource+Allocation+for+Jobs+and+Applications) of the MapR documentation. Remaining memory is given to YARN applications. 


### MapReduce v2 and other Resources

You configure memory for each service by setting three values in `warden.conf`.

    service.command.<servicename>.heapsize.percent
    service.command.<servicename>.heapsize.max
    service.command.<servicename>.heapsize.min

Configure memory for other services in the same manner, as described in [MapR documentation](http://doc.mapr.com/display/MapR/warden.%3Cservicename%3E.conf)

For more information about managing memory in a MapR cluster, see the following sections in the MapR documentation:

* [Memory Allocation for Nodes](http://doc.mapr.com/display/MapR/Memory+Allocation+for+Nodes)  
* [Cluster Resource Allocation](http://doc.mapr.com/display/MapR/Cluster+Resource+Allocation)  
* [Customizing Memory Settings for MapReduce v1](http://doc.mapr.com/display/MapR/Customize+Memory+Settings+for+MapReduce+v1)  

## How to Manage Drill CPU Resources
Currently, you do not manage CPU resources within Drill. [Use Linux `cgroups`](http://en.wikipedia.org/wiki/Cgroups) to manage the CPU resources.