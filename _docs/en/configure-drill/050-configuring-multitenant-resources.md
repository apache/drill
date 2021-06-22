---
title: "Configuring Multitenant Resources"
slug: "Configuring Multitenant Resources"
parent: "Configuring a Multitenant Cluster"
---
Drill operations are memory and CPU-intensive. Currently, Drill resources are managed outside of any cluster management service. In a multitenant or any other type of cluster, YARN-enabled or not, you configure memory and memory usage limits for Drill by modifying the `drill-env.sh` file as described in ["Configuring Drill Memory"]({{site.baseurl}}/docs/configuring-drill-memory).

Configure a multitenant cluster manager to account for resources required for Drill. Configuring `drill-env.sh` allocates resources for Drill to use during query execution. It might be necessary to configure the cluster manager from committing the resources to other processes.

## Configuring Drill in a YARN-enabled cluster

To add Drill to a YARN-enabled cluster, change memory resources to suit your application. For example, you have 128G of available memory that you allocate to following workloads in a Yarn-enabled cluster:

File system = 20G  
HBase = 20G  
OS = 8G  
Yarn = ?
Drill = ?

If Yarn does most of the work, give Drill 20G, for example, and give Yarn 60G. If you expect a heavy query load, give Drill 60G and Yarn 20G.

YARN consists of two main services:

* ResourceManager  
  There is at least one instance in a cluster, more if you configure high availability.  
* NodeManager  
  There is one instance per node. 

Configure NodeManager and ResourceManager to reconfigure the total memory required for YARN services to run. If you want to place an upper limit on memory set YARN_NODEMANAGER_HEAPSIZE or YARN_RESOURCEMANAGER_HEAPSIZE environment variable. Do not set the `-Xmx` option to allow the heap to grow as needed.

### MapReduce Resources

Modify MapReduce memory to suit your application needs. Remaining memory is typically given to YARN applications. 

## How to Manage Drill CPU Resources
Currently, you cannot manage CPU resources within Drill. [Use Linux `cgroups`](http://en.wikipedia.org/wiki/Cgroups) to manage the CPU resources.

## How to Manage Disk Resources

The `planner.add_producer_consumer` system option enables or disables a secondary reading thread that works out of band of the rest of the scanning fragment to prefetch data from disk. If you interact with a certain type of storage medium that is slow or does not prefetch much data, this option tells Drill to add a producer consumer reading thread to the operation. Drill can then assign one thread that focuses on a single reading fragment. If Drill is using memory, you can disable this option to get better performance. If Drill is using disk space, you should enable this option and set a reasonable queue size for the `planner.producer_consumer_queue_size` option. For more information about these options, see the section, ["Performance Tuning"](/docs/performance-tuning-introduction/).
