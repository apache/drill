---
title: "Configuring Memory for Drill in a Drill cluster"
parent: "Manage Drill"
---

This section describes how to configure the amount of direct memory allocated to a Drillbit for query processing in a dedicated Drill cluster. When you use Drill in a cluster with other workloads, configure memory as described in section, ["Configuring Resources in a Cluster"]({{site.baseurl}}/docs/configuring-resources-in-a-cluster). 

The default memory for a Drillbit is 8G, but Drill prefers 16G or more
depending on the workload. The total amount of direct memory that a Drillbit
allocates to query operations cannot exceed the limit set.

Drill mainly uses Java direct memory and performs well when executing
operations in memory instead of storing the operations on disk. Drill does not
write to disk unless absolutely necessary, unlike MapReduce where everything
is written to disk during each phase of a job.

The JVM’s heap memory does not limit the amount of direct memory available in
a Drillbit. The on-heap memory for Drill is only about 4-8G, which should
suffice because Drill avoids having data sit in heap memory.

## Modifying Drillbit Memory

You can modify memory for each Drillbit node in your cluster. To modify the
memory for a Drillbit, edit the `XX:MaxDirectMemorySize` parameter in the
Drillbit startup script located in `<drill_installation_directory>/conf/drill-
env.sh`.

{% include startnote.html %}If this parameter is not set, the limit depends on the amount of available system memory.{% include endnote.html %}

After you edit `<drill_installation_directory>/conf/drill-env.sh`, [restart the Drillbit]({{ site.baseurl }}/docs/starting-stopping-drill#starting-a-drillbit) onthe node.