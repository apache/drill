---
title: "Configuring a Cluster for Different Workloads"
parent: "Manage Drill"
---
In this release of Drill, to configure a Drill cluster for different workloads, you re-allocate memory resources only. Warden allocates resources for MapR Hadoop services associated with roles that are installed on the node. For example, warden allocates memory for MapR Hadoop services, such as Zookeeper and NFS. You modify `warden.conf` to manage memory allocation. For example, you re-allocate memory for the following services by modifying these settings: 

    service.command.nfs.heapsize.percent=3
    service.command.nfs.heapsize.min=64
    service.command.nfs.heapsize.max=64
    . . .
    service.command.zk.heapsize.percent=1
    service.command.zk.heapsize.max=1500
    service.command.zk.heapsize.min=256

First, establish baselines for performance testing. Next, make changes to memory allocation properties in small increments. Finally, test and compare the effects of the change.

Memory and disk for Drill and other services that are not associated with roles on a MapR cluster are shared with other services. You manage the chunk of memory for these services in os heap settings in `warden.conf` and in configuration files of the particular service. The warden os heap settings are:

    service.command.os.heapsize.percent
    service.command.os.heapsize.max
    service.command.os.heapsize.min

## Allocating Memory for JobTracker

Memory allocated for JobTracker in the warden.conf is used only to calculate total memory required for services to run. The -Xmx JobTracker itself is not set, allowing memory on JobTracker to grow as needed. If you want to set an upper limit on memory, set the HADOOP_HEAPSIZE env. variable in `/opt/mapr/hadoop/hadoop-0.20.2/conf/hadoop-env.sh`.

## Allocating Memory for Drill
You can configure the amount of direct memory allocated to a Drillbit for
query processing. The default limit is 8G, but Drill prefers 16G or more
depending on the workload. The total amount of direct memory that a Drillbit
allocates to query operations cannot exceed the limit set.

Drill mainly uses Java direct memory and performs well when executing
operations in memory instead of storing the operations on disk. Drill does not
write to disk unless absolutely necessary, unlike MapReduce where everything
is written to disk during each phase of a job.

The JVM’s heap memory does not limit the amount of direct memory available in
a Drillbit. The on-heap memory for Drill is only about 4-8G, which should
suffice because Drill avoids having data sit in heap memory.

You can modify memory for each Drillbit node in your cluster. To modify the
memory for a Drillbit, edit the `XX:MaxDirectMemorySize` parameter in the
Drillbit startup script located in `<drill_installation_directory>/conf/drill-
env.sh`.

{% include startnote.html %}If this parameter is not set, the limit depends on the amount of available system memory.{% include endnote.html %}

After you edit `<drill_installation_directory>/conf/drill-env.sh`, [restart
the Drillbit
]({{ site.baseurl }}/docs/starting-stopping-drill#starting-a-drillbit)on
the node.

