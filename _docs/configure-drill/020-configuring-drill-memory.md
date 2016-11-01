---
title: "Configuring Drill Memory"
date: 2016-11-01 21:03:43 UTC
parent: "Configure Drill"
---

You can configure the amount of direct memory allocated to a Drillbit for query processing in any Drill cluster, multitenant or not. The default memory for a Drillbit is 8G, but Drill prefers 16G or more depending on the workload. The total amount of direct memory that a Drillbit allocates to query operations cannot exceed the limit set.

Drill uses Java direct memory and performs well when executing
operations in memory instead of storing the operations on disk. Drill does not
write to disk unless absolutely necessary, unlike MapReduce where everything
is written to disk during each phase of a job.

The JVM’s heap memory does not limit the amount of direct memory available in
a Drillbit. The on-heap memory for Drill is typically set at 4-8G (default is 4), which should
suffice because Drill avoids having data sit in heap memory.

As of Drill 1.5, Drill uses a new allocator that improves an operator’s use of direct memory and tracks the memory use more accurately. Due to this change, the sort operator (in queries that ran successfully in previous releases) may not have enough memory, resulting in a failed query and out of memory error instead of spilling to disk.


The [`planner.memory.max_query_memory_per_node`]({{site.baseurl}}/docs/configuration-options-introduction/#system-options) system option value sets the maximum amount of direct memory allocated to the sort operator in each query on a node. If a query plan contains multiple sort operators, they all share this memory. If you encounter memory issues when running queries with sort operators, increase the value of this option. If you continue to encounter memory issues after increasing this value, you can also reduce the value of the [`planner.width.max_per_node`]({{site.baseurl}}/docs/configuration-options-introduction/) option to reduce the level of parallelism per node. However, this may increase the amount of time required for a query to complete.  


## Modifying Drillbit Memory

You can modify memory for each Drillbit node in your cluster. To modify the memory for a Drillbit, set the DRILL_MAX_DIRECT_MEMORY variable in the Drillbit startup script, `drill-env.sh`, located in `<drill_installation_directory>/conf`, as follows:

    export DRILL_MAX_DIRECT_MEMORY=${DRILL_MAX_DIRECT_MEMORY:-"<value>"}

{% include startnote.html %}If DRILL_MAX_DIRECT_MEMORY is not set, the limit depends on the amount of available system memory.{% include endnote.html %}

After you edit `<drill_installation_directory>/conf/drill-env.sh`, [restart the Drillbit]({{ site.baseurl }}/docs/starting-drill-in-distributed-mode) on the node.

## About the Drillbit startup script

The `drill-env.sh` file contains the following options:

    #export DRILL_HEAP=${DRILL_HEAP:-"4G”}  
    #export DRILL_MAX_DIRECT_MEMORY=${DRILL_MAX_DIRECT_MEMORY:-"8G"}

To customize memory limits, uncomment the line needed and change the setting:  

    export DRILL_HEAP=${DRILL_HEAP:-"<limit>”}
    export DRILL_MAX_DIRECT_MEMORY=${DRILL_MAX_DIRECT_MEMORY:-“<limit>"}  

DRILL_MAX_HEAP is the maximum theoretical heap limit for the JVM per node.  
DRILL_MAX_DIRECT_MEMORY is the Java direct memory limit per node.  

If performance is an issue, add -Dbounds=false, as shown in the following example:

    export DRILL_JAVA_OPTS="$DRILL_JAVA_OPTS -Dbounds=false"
