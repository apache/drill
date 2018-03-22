---
title: "Configuring Drill Memory"
date: 2018-03-22 18:52:44 UTC
parent: "Configure Drill"
---

Drill uses Java direct memory. You can configure the amount of direct memory allocated to a Drillbit for query processing. The default memory for a Drillbit is 8G, but Drill prefers 16G or more depending on the workload. The total amount of direct memory that a Drillbit allocates to query operations cannot exceed the limit set.

Drill performs well when executing operations in memory instead of storing the operations on disk. Drill does not write to disk unless absolutely necessary, unlike MapReduce where everything is written to disk during each phase of a job.

The JVM heap memory does not limit the amount of direct memory available in a Drillbit. The on-heap memory for Drill is typically set at 4-8G (default is 4), which should
suffice because Drill avoids having data sit in heap memory.  

The following sections describe how to allocate memory to Drillbits and queries, and how to enable bounds checking if you see performance issues:  

## Modifying Memory Allocated to a Drillbit  

Modify the memory allocated to each Drillbit in a cluster in the Drillbit startup script, `<drill_installation_directory>/conf/drill-env.sh`. You must [restart Drill]({{ site.baseurl }}/docs/starting-drill-in-distributed-mode) after you modify the script.  

The `drill-env.sh` file contains the following options:

    #export DRILLBIT_MAX_PROC_MEM=${DRILLBIT_MAX_PROC_MEM:-"13G"}
    //Maximum cumulative memory allocated to the Drill process during startup. This option was introduced in Drill 1.13.

    #export DRILL_HEAP=${DRILL_HEAP:-"4G"}
    //Maximum theoretical heap limit for the JVM per node.

    #export DRILL_MAX_DIRECT_MEMORY=${DRILL_MAX_DIRECT_MEMORY:-"8G"}  
    //Java direct memory limit per node.

    #export DRILLBIT_CODE_CACHE_SIZE=${DRILLBIT_CODE_CACHE_SIZE:-"1G"} 
    //Do not modify the DRILLBIT_CODE_CACHE_SIZE. The value for this parameter is auto-computed based on the heap size and cannot exceed 1GB. 

{% include startnote.html %}If DRILL_MAX_DIRECT_MEMORY is not set, the limit depends on the amount of available direct memory.{% include endnote.html %}


To customize memory limits, uncomment the line needed and change the setting:  

    export DRILLBIT_MAX_PROC_MEM=${DRILLBIT_MAX_PROC_MEM:-"<limit>"}
    export DRILL_HEAP=${DRILL_HEAP:-"<limit>"}
    export DRILL_MAX_DIRECT_MEMORY=${DRILL_MAX_DIRECT_MEMORY:-"<limit>"}   


For example, if you set `DRILLBIT_MAX_PROC_MEM` to 40G, the total amount of memory allocated to the following memory parameters cannot exceed 40G:  

       DRILL_HEAP=8G
       DRILL_MAX_DIRECT_MEMORY=10G
       DRILLBIT_CODE_CACHE_SIZE=1024M

At startup, the auto-setup.sh script (introduced in Drill 1.13) performs a check to see if these memory parameters are declared. If the parameters are declared, the script performs a check to verify that the cumulative memory of the parameters does not exceed the value specified by `DRILLBIT_MAX_PROC_MEM`. If the cumulative memory exceeds the total amount of memory defined by `DRILLBIT_MAX_PROC_MEM`, Drill returns an error message with instructions. See the example below.

If any of the memory parameters are undefined, the script adjusts the settings for the undefined parameters to ensure that the total memory allocated is within the value defined by `DRILLBIT_MAX_PROC_MEM`.

By default, `DRILLBIT_MAX_PROC_MEM` is not defined. You can define `DRILLBIT_MAX_PROC_MEM` in KB, MB, or GB, as shown:  

       DRILLBIT_MAX_PROC_MEM=13G
       DRILLBIT_MAX_PROC_MEM=8192M
       DRILLBIT_MAX_PROC_MEM=4194304K

Alternatively, you can set `DRILLBIT_MAX_PROC_MEM` as a percentage of total memory:  

       DRILLBIT_MAX_PROC_MEM=50%

If you do not set this variable, it is disabled. If you set this variable, you can unset it to disable it.  

**Example**  

If a system has 48GB of free memory and you set the following parameters in drill-env.sh:  

       DRILLBIT_MAX_PROC_MEM=25%
       DRILL_HEAP=8G
       DRILL_MAX_DIRECT_MEMORY=10G
       DRILLBIT_CODE_CACHE_SIZE=1024M  

The Drillbit fails on startup with the following messages:

       [WARN] 25% of System Memory (47 GB) translates to 12 GB
       [ERROR] Unable to start Drillbit due to memory constraint violations Total Memory Requested : 19 GB 
       Check and modify the settings or increase the maximum amount of memory permitted.

If DRILLBIT_MAX_PROC_MEM is increased to 50%; the Drillbit starts up with the following warnings:  

       [WARN] 50% of the system memory (48 GB) translates to 24 GB.
       [WARN] You have an allocation of 4 GB that is currently unused from a total of 24 GB. 
       You can increase your existing memory configuration to use this extra memory.  

Additionally, if the available free memory is less than the allocation, the following additional warnings are provided under the assumption that the operating system will reclaim more free memory when required:

       [WARN] Total Memory Allocation for Drillbit (19GB) exceeds available free memory (11GB).
       [WARN] Drillbit will start up, but can potentially crash due to oversubscribing of system memory.  
  

##Modifying Memory Allocated to Queries  

You can configure the amount of memory that Drill allocates to each query as a hard limit or a percentage of the total direct memory. The `planner.memory.max_query_memory_per_node` and `planner.memory.percent_per_query` options set the amount of memory that Drill can allocate to a query on a node. Both options are enabled by default. Of these two options, Drill picks the setting that provides the most memory. For more information about these options, see [Sort-Based and Hash-Based Memory Constrained Operators](https://drill.apache.org/docs/sort-based-and-hash-based-memory-constrained-operators/).  


If you modify the memory allocated per query and continue to experience out-of-memory errors, you can try reducing the value of the [`planner.width.max_per_node`]({{site.baseurl}}/docs/configuration-options-introduction/) option. Reducing the value of this option reduces the level of parallelism per node. However, this may increase the amount of time required for a query to complete.  

You can also modify the `drill.exec.memory.operator.output_batch_size` option, introduced in Drill 1.13. The `drill.exec.memory.operator.output_batch_size` option limits the amount of memory that the Flatten, Merge Join, and External Sort operators allocate to outgoing batches. Limiting the memory allocated to outgoing batches can improve concurrency and prevent queries from failing with out-of-memory errors.
 
The average row size of the outgoing batch (calculated from the incoming batch size) determines the number of rows that can fit into the available memory for the batch. If your queries fail with memory errors, reduce the value of the `drill.exec.memory.operator.output_batch_size` option to reduce the output batch size. 

The default value is 16777216 (16 MB). The maximum allowed value is 536870912 (512 MB). Enter the value in bytes. 

**Note:** Configuring a batch size less than 1 MB is not recommended, as it could lead to performance issues. 

Use the ALTER SYSTEM SET command to change the settings, as shown:  

       ALTER SYSTEM SET `drill.exec.memory.operator.output_batch_size` = <value>;  

##Bounds Checking 

If performance is an issue, add -Dbounds=false, as shown in the following example:

    export DRILL_JAVA_OPTS="$DRILL_JAVA_OPTS -Dbounds=false"  

As of Drill 1.13, bounds checking for direct memory is disabled by default. To enable bounds checking for direct memory, use the DRILLBIT_JAVA_OPTS variable to pass the `drill.exec.memory.enable_unsafe_bounds_check` parameter in $DRILL_HOME/conf/drill-env.sh, as shown:  

    export DRILL_JAVA_OPTS="$DRILL_JAVA_OPTS -Ddrill.exec.memory.enable_unsafe_bounds_check=true"  


For earlier versions of Drill (prior to 1.13), bounds checking is enabled by default. To disable bounds checking, set the `drill.enable_unsafe_memory_access` parameter to true, as shown:  


    export DRILL_JAVA_OPTS="$DRILL_JAVA_OPTS -Ddrill.enable_unsafe_memory_access=true"
  