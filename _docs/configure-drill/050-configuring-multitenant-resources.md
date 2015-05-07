---
title: "Configuring Multitenant Resources"
parent: "Configuring a Multitenant Cluster"
---
Drill operations are memory and CPU-intensive. Currently, Drill resources are managed outside MapR Warden service in terms of configuring the resources as well as enforcing the resource usage within the limit. Configure memory for Drill in a multitenant by modifying drill-env.sh. <Add detail on property names etc>

3. To ensure Warden account for resources required for Drill, make sure the following properties are set appropriately in warden.drill-bits.conf. For reference on the meaning of the properties refer to the following MapR doc <add link>

service.heapsize.min
service.heapsize.max
service.heapsize.percent

The percent should always be considered as the one to change, it is more intuitive to understand and grows/shrinks according to different node's configuration. 

It will be good if someone in Drill QA could try it out and see if it fits Drill's needs. 

 

Note that these properties should be set in addition to configuring the resources the in drill-env.conf even if you didnt change the defaults in drill-env.sh. Setting up memory limit in drill-env.sh tells Drill how much resources to use during query execution and setting up these warden-drill-bits.conf tells warden not to commit these resources to some other process. In near future, we expect to provide a more deeper integration on these settings
\<give an example>

4. This configuration is same whether you use Drill is enabled cluster or non-YARN cluster.




You need to statically partition the cluster to designate which partition handles which workload. To configure resources for Drill in a MapR cluster, modify one or more of the following files in `/opt/mapr/conf/conf.d` that the installation process creates. 

* `warden.drill-bits.conf`
* `warden.nodemanager.conf`
* `warden.resourcemanager.conf`

Configure Drill memory by modifying `warden.drill-bits.conf` in YARN and non-YARN clusters. Configure other resources by modifying `warden.nodemanager.conf `and `warden.resourcemanager.conf `in a YARN-enabled cluster.

## Configuring Drill Memory in a Mixed Cluster

Add the following lines to the `warden.drill-bits.conf` file to configure memory resources for Drill:

    service.heapsize.min=<some value in MB>
    service.heapsize.max=<some value in MB>
    service.heapsize.percent=<a whole number>

The service.heapsize.percent is the percentage of memory for the service bounded by minimum and maximum values.

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

Change these settings for NodeManager and ResourceManager to reconfigure the total memory required for YARN services to run. If you want to place an upper limit on memory set YARN_NODEMANAGER_HEAPSIZE or YARN_RESOURCEMANAGER_HEAPSIZE environment variable in /opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop/yarn-env.sh. The -Xmx option is not set, allowing memory on to grow as needed.

### MapReduce v1 Resources

The following default settings in /opt/mapr/conf/warden.conf control MapReduce v1 memory:

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

* [Memory Allocation for Nodes](http://doc.mapr.com/display/MapR40x/Memory+Allocation+for+Nodes)  
* [Cluster Resource Allocation](http://doc.mapr.com/display/MapR40x/Cluster+Resource+Allocation)  
* [Customizing Memory Settings for MapReduce v1](http://doc.mapr.com/display/MapR40x/Customize+Memory+Settings+for+MapReduce+v1)  

## How to Manage Drill CPU Resources
Currently, you do not manage CPU resources within Drill. [Use Linux `cgroups`](http://en.wikipedia.org/wiki/Cgroups) to manage the CPU resources.