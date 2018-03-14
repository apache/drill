---
title: "Configuring Drill Memory"
date: 2018-03-14 00:58:05 UTC
parent: "Configure Drill"
---

Drill uses Java direct memory. You can configure the amount of direct memory allocated to a Drillbit for query processing. The default memory for a Drillbit is 8G, but Drill prefers 16G or more depending on the workload. The total amount of direct memory that a Drillbit allocates to query operations cannot exceed the limit set.

Drill performs well when executing operations in memory instead of storing the operations on disk. Drill does not write to disk unless absolutely necessary, unlike MapReduce where everything is written to disk during each phase of a job.

The JVM heap memory does not limit the amount of direct memory available in a Drillbit. The on-heap memory for Drill is typically set at 4-8G (default is 4), which should
suffice because Drill avoids having data sit in heap memory.  

The following sections describe how to modify the memory allocated to each Drillbit and queries:  

## Modifying Memory Allocated to a Drillbit  

Modify the memory allocated to each Drillbit in a cluster in the Drillbit startup script, `<drill_installation_directory>/conf/drill-env.sh`. You must [restart Drill]({{ site.baseurl }}/docs/starting-drill-in-distributed-mode) after you modify the script.

{% include startnote.html %}If DRILL_MAX_DIRECT_MEMORY is not set, the limit depends on the amount of available direct memory.{% include endnote.html %}


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

As of Drill 1.13, bounds checking for direct memory is disabled by default. To enable bounds checking for direct memory, use the DRILLBIT_JAVA_OPTS variable to pass the `drill.exec.memory.enable_unsafe_bounds_check` parameter in $DRILL_HOME/conf/drill-env.sh, as shown:  

    export DRILL_JAVA_OPTS="$DRILL_JAVA_OPTS -Ddrill.exec.memory.enable_unsafe_bounds_check=true"  


For earlier versions of Drill (prior to 1.13), bounds checking is enabled by default. To disable bounds checking, set the `drill.enable_unsafe_memory_access` parameter to true, as shown:  


    export DRILL_JAVA_OPTS="$DRILL_JAVA_OPTS -Ddrill.enable_unsafe_memory_access=true"  


##Modifying Memory Allocated to Queries  

You can configure the amount of memory that Drill allocates to each query as a hard limit or a percentage of the total direct memory. The `planner.memory.max_query_memory_per_node` and `planner.memory.percent_per_query` options set the amount of memory that Drill can allocate to a query on a node. Both options are enabled by default. Of these two options, Drill picks the setting that provides the most memory. For more information about these options, see [Sort-Based and Hash-Based Memory Constrained Operators](https://drill.apache.org/docs/sort-based-and-hash-based-memory-constrained-operators/).  


If you modify the memory allocated per query and continue to experience out-of-memory errors, you can try reducing the value of the [`planner.width.max_per_node`]({{site.baseurl}}/docs/configuration-options-introduction/) option. Reducing the value of this option reduces the level of parallelism per node. However, this may increase the amount of time required for a query to complete.  

Another option you can modify is the `drill.exec.memory.operator.output_batch_size` option, introduced in Drill 1.13. The  `drill.exec.memory.operator.output_batch_size` option limits the amount of memory that the Flatten, Merge Join, and External Sort operators allocate to outgoing batches. Limiting the memory allocated to outgoing batches can improve concurrency and prevent queries from failing with out-of-memory errors.
 
The average row size of the outgoing batch (calculated from the incoming batch size) determines the number of rows that can fit into the available memory for the batch. If your queries fail with memory errors, reduce the value of the `drill.exec.memory.operator.output_batch_size` option to reduce the output batch size. 

The default value is 16777216 (16 MB). The maximum allowed value is 536870912 (512 MB). Enter the value in bytes. 

**Note:** Configuring a batch size less than 1 MB is not recommended, as it could lead to performance issues. 

Use the ALTER SYSTEM SET command to change the settings, as shown:  

       ALTER SYSTEM SET `drill.exec.memory.operator.output_batch_size` = <value>;
  