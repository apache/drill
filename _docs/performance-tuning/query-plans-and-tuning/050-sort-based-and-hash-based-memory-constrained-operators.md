---
title: "Sort-Based and Hash-Based Memory-Constrained Operators"
date: 2017-08-17 04:49:34 UTC
parent: "Query Plans and Tuning"
--- 

Drill uses hash-based and sort-based operators depending on the query characteristics. Hash aggregation and hash join are hash-based operations. Streaming aggregation and merge join are sort-based operations. Both hash-based and sort-based operations consume memory, however the hash aggregate and hash join operators are the fastest and most memory intensive operators. 

When planning a query with sort- and hash-based operators, Drill evaluates the available memory multiplied by a configurable reduction constant (for parallelization purposes) and then limits the operations to the maximum of this amount of memory. Drill spills data to disk if the sort and hash aggregate operations cannot be performed in memory. Alternatively, you can disable large hash operations if they do not fit in memory on your system. When disabled, Drill creates alternative plans. You can also modify the minimum hash table size, increasing the size for very large aggregations or joins when you have large amounts of memory for Drill to use. If you have large data sets, you can increase the hash table size to improve performance. 

##Memory Options
The `planner.memory.max_query_memory_per_node` option sets the maximum amount of direct memory allocated to the sort and hash aggregate operators during each query on a node. The default limit is 2147483648 bytes (2GB), which is quite conservative. This memory is split between operators. If a query plan contains multiple sort and/or hash aggregate operators, the memory is divided between them.

When a query is parallelized, the number of operators is multiplied, which reduces the amount of memory given to each instance of the sort and hash aggregate operators during a query. If you encounter memory issues when running queries with sort and hash aggregate operators, calculate the memory requirements for your queries and the amount of available memory on each node. Based on the information, increase the value for the `planner.memory.max_query_memory_per_node` option using the ALTER SYSTEM|SESSION SET command, as shown:  

    ALTER SYSTEM|SESSION SET `planner.memory.max_query_memory_per_node` = 8147483648  
  

The `planner.memory.enable_memory_estimation` option toggles the state of memory estimation and re-planning of the query. When enabled, Drill conservatively estimates memory requirements and typically excludes memory-constrained operators from the query plan, which can negatively impact performance. The default setting is false. If you want Drill to use very conservative memory estimates, use the ALTER SYSTEM|SESSION SET command to change the setting, as shown:  

    ALTER SYSTEM|SESSION SET `planner.memory.enable_memory_estimation` = true  

 
##Spill to Disk  
The "Spill to Disk" feature prevents queries that use memory-intensive sort and hash aggregate operations from failing with out-of-memory errors. Drill automatically writes excess data to a temporary directory on disk when queries with sort or hash aggregate operations exceed the set memory limit on a Drill node. When the operators finish processing the in-memory data, Drill reads the spilled data back from disk, and the operators finish processing the data. When the operations complete, Drill removes the data from disk.  

Spilling to disk enables queries to run uninterrupted while Drill performs the spill operations in the background. However, there can be performance impact due to the time required to spill data and then read the data back from disk.  

{% include startnote.html %}Drill 1.11 and later supports spilling to disk for the Hash Aggregate operator in addition to the Sort operator. Previous releases of Drill only supported spilling to disk for the Sort operator.{% include endnote.html %}  

###Spill Locations  
Drill writes data to a temporary work area on disk. The default location of the temporary work area is /tmp/drill/spill on the local file system. The /tmp/drill/spill directory should suffice for small workloads or examples, however it is highly recommended that you redirect the default spill location to a location with enough disk space to support spilling for large workloads.  
 
{% include startnote.html %}Spilled data may require more space than the table referenced in the query that is spilling the data. For example, if a table is 100 GB per node, the spill directory should have the capacity to hold more than 100 GB.{% include endnote.html %}
 
When you configure the spill location, you can specify a single directory, or a list of directories into which the sort and hash aggregate operators both spill. Alternatively, you can set specific spill directories for each type of operator, however this is not recommended as these options will be deprecated in future releases of Drill. For more information, see the Spill to Disk Configuration Options section below.  

###Spill to Disk Configuration Options  
The spill to disk options reside in the drill-override.conf file on each Drill node. An administrator or someone familiar with storage and disks should manage these settings.

{% include startnote.html %}You can see examples of these configuration options in the drill-override-example.conf file located in the <drill_installation>/conf directory.{% include endnote.html %} 

The following list describes the configuration options for spilling data to disk:  

* **drill.exe.spill.fs**  
Introduced in Drill 1.11. The default file system on the local machine into which the sort and hash aggregate operators spill data. This is the recommended option to use for spilling. You can configure this option so that data spills into a distributed file system, such as hdfs. For example, "hdfs:///". The default setting is "file:///".  
  
* **drill.exec.spill.directories**  
Introduced in Drill 1.11. The list of directories into which the sort and hash aggregate operators spill data. The list must be an array with directories separated by a comma, for example ["/fs1/drill/spill" , "/fs2/drill/spill" , "/fs3/drill/spill"]. This is the recommended option for spilling to multiple directories. The default setting is ["/tmp/drill/spill"].  
  
* **drill.exec.sort.external.spill.fs**    
Overrides the default location into which the sort operator spills data. Instead of spilling into the location set by the drill.exec.spill.fs option, the sort operators spill into the location specified by this option.  
**Note:** As of Drill 1.11, this option is supported for backward compatibility, however in future releases, this option will be deprecated. It is highly recommended that you   use the drill.exec.spill.fs option to set the spill location instead. The default setting is "file:///".
* **drill.exec.sort.external.spill.directories**   
Overrides the location into which the sort operator spills data. Instead of spilling into the location set by the drill.exec.spill.directories option, the sort operators spill into the directories specified by this option. The list must be an array with directories separated by a comma, for example ["/fs1/drill/spill" , "/fs2/drill/spill" , "/fs3/drill/spill"].  
**Note:** As of Drill 1.11, this option is supported for backward compatibility, however in future releases, this option will be deprecated. It is highly recommended that you use the drill.exec.spill.directories option to set the spill location instead. The default setting is ["/tmp/drill/spill"].  
 
* **drill.exec.hashagg.spill.fs**  
Overrides the location into which the hash aggregate operator spills data. Instead of spilling into the location set by the drill.exec.spill.fs option, the hash aggregate operator spills into the location specified by this option. Setting this option to 1 disables spilling for the hash aggregate operator.  
**Note:** As of Drill 1.11, this option is supported for backward compatibility, however in future releases, this option will be deprecated. It is highly recommended that you use the drill.exec.spill.fs option to set the spill location instead. The default setting is "file:///".  
  
* **drill.exec.hashagg.spill.directories**    
Overrides the location into which the hash aggregate operator spills data. Instead of spilling into the location set by the drill.exec.spill.directories option, the hash aggregate operator spills to the directories specified by this option. The list must be an array with directories separated by a comma, for example ["/fs1/drill/spill" , "/fs2/drill/spill" , "/fs3/drill/spill"].  
**Note:** As of Drill 1.11, this option is supported for backward compatibility, however in future releases, this option will be deprecated. It is highly recommended that you use the drill.exec.spill. directories option to set the spill location instead.  


##Hash-Based Operator Settings
Use the ALTER SYSTEM|SESSION SET commands with the options below to disable the hash aggregate and hash join operators, modify the hash table size, disable memory estimation, or set the estimated maximum amount of memory for a query. Typically, you set the options at the session level unless you want the setting to persist across all sessions.

The following options control the hash-based operators:

* **planner.enable_hashagg**  
    Enables or disables hash aggregation; otherwise, Drill does a sort-based aggregation. This option is enabled by default.   The default setting is true, which is recommended.

* **planner.enable_hashjoin**  
    Enables or disables the memory hungry hash join. Drill assumes that a query will have adequate memory to complete and tries to use the fastest operations possible to complete the planned inner, left, right, or full outer joins using a hash table. Currently, this operator does not write to disk. Disabling hash join allows Drill to manage arbitrarily large data in a small memory footprint. This option is enabled by default. The default setting is true.

* **exec.min_hash_table_size**  
    Starting size for hash tables. Increase this setting based on the memory available to improve performance.  
    The default setting for this option is 65536. The setting can range from 0 to 1073741824.

* **exec.max\_hash\_table_size**  
    Ending size for hash tables. The default setting for this option is 1073741824. The setting can range from 0 to 1073741824.


  




