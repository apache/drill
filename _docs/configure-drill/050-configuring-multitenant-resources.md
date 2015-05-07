---
title: "Configuring Multitenant Resources"
parent: "Configuring a Multitenant Cluster"
---
Drill operations are memory and CPU-intensive. Currently, Drill resources are managed outside of any cluster management service. In a multitenant or any other type of cluster, YARN-enabled or not, you configure memory and memory usage limits for Drill by modifying drill-env.sh as described in ["Configuring Drill Memory"]({{site.baseurl}}/docs/configuring-drill-memory).

Configure a multitenant cluster manager to account for resources required for Drill. Configuring `drill-env.sh` allocates resources for Drill to use during query execution. It might be necessary to configure the cluster manager from committing the resources to other processes.

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

Change these settings for NodeManager and ResourceManager to reconfigure the total memory required for YARN services to run. If you want to place an upper limit on memory set YARN_NODEMANAGER_HEAPSIZE or YARN_RESOURCEMANAGER_HEAPSIZE environment variable. Do not set the `-Xmx` option to allow the heap to grow as needed.

### MapReduce Resources

Modify MapReduce memory to suit your application needs. Remaining memory is typically given to YARN applications. 

## How to Manage Drill CPU Resources
Currently, you do not manage CPU resources within Drill. [Use Linux `cgroups`](http://en.wikipedia.org/wiki/Cgroups) to manage the CPU resources.