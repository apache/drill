---
title: "Sort-Based and Hash-Based Memory-Constrained Operators"
date: 2018-06-20 01:48:01 UTC
parent: "Query Plans and Tuning"
--- 

Drill uses operators to sort, join, and aggregate data when executing queries. Drill uses the Sort operator to sort data. Drill can use the Hash Aggregate or Hash Join operators to aggregate data, or Drill can sort the data and then use the Merge Join or Streaming Aggregate operators to aggregate the data. 

The Hash operators typically perform better, however they are more memory intensive than the Merge Join and Streaming Aggregate operators. The Sort operator may use as much or even more memory than the Hash operators. If you want to see the difference in memory consumption between the operators, you can run a query and view the query profile in the Drill Web Console. Optionally, you can disable the Hash operators to force Drill to use the Merge Join and Streaming Aggregate operators. 

When a query requires sorting, joining, and aggregation, Drill equally divides the memory available among each instance of these memory intensive operators in a query. The number of instances is equivalent to the number of these operators in the query plan, each multiplied by its degree of parallelism. The degree of parallelism is the number of minor fragments required to perform the work for each instance of an operator. When an instance of an operator must process more data than it can hold, the operator temporarily spills some of the data to a directory on disk to complete its work.  


##Spill to Disk  

Spilling to disk prevents queries that use memory intensive operations from failing with out-of-memory errors. The Spill to Disk feature enables the Sort, Hash Aggregate, and Hash Join operators to automatically write excess data (as files) to a temporary directory on disk when the memory requirements for the operators exceed the set memory limit. Queries run uninterrupted while the operators perform the spill operations in the background.

When the Sort, Hash Aggregate, and Hash Join operators finish processing the data in memory, they read the spilled data back from disk and then finish processing the data. The operators clean up their data (files) from the temporary spill location after they finish processing the data. 

Ideally, you want to allocate enough memory for Drill to perform all operations in memory. When data spills to disk, you will not see any difference in terms of how queries run, however spilling to disk can impact performance due to the additional I/O required to write data to disk and read the data back. See Memory Allocation (page 4) for more information. 

**Note:** Drill 1.14 and later supports spilling to disk for the Hash Join, Hash Aggregate, and Sort operators. Drill 1.11, 1.12, and 1.13 supports spilling to disk for the Hash Aggregate and Sort operators. Releases of Drill prior to 1.11 only support spilling to disk for the Sort operator.  

**Spill Locations** 

The Sort, Hash Aggregate, and Hash Join operators write data to a temporary work area on disk when they cannot process all of the data in memory. The default location of the temporary work area is /tmp/drill/spill on the local file system. 

The /tmp/drill/spill directory should suffice for small workloads or examples, however it is highly recommended that you redirect the default spill location to a location with enough disk space to support spilling for large workloads.

**Note:** Spilled data may require more space than the table referenced in the query that is spilling the data. For example, if a table is 100 GB per node, the spill directory should have the capacity to hold more than 100 GB.

When you configure the spill location, you can specify a single directory or a list of directories into which the Sort, Hash Aggregate, and Hash Join operators spill data. For more information, see the Spill to Disk Configuration Options section below.  

**Spill to Disk Configuration Options**  

The drill-override.conf file, located in the /conf directory, contains options that set the spill locations for the Hash and Sort operators. An administrator can change the file system and directories into which the operators spill data. Refer to the drill-override-example.conf file for examples. 

The following list describes the spill to disk configuration options:  

- **drill.exec.spill.fs**  
Introduced in Drill 1.11. The default file system on the local machine into which the Sort, Hash Aggregate, and Hash Join operators spill data. You can configure this option so that data spills into a distributed file system, such as hdfs. For example, "hdfs:///". The default setting is "file:///".
- **drill.exec.spill.directories**  
Introduced in Drill 1.11. The list of directories into which the Sort, Hash Aggregate, and Hash Join operators spill data. The list must be an array with directories separated by a comma, for example ["/fs1/drill/spill" , "/fs2/drill/spill" , "/fs3/drill/spill"]. The default setting is ["/tmp/drill/spill"].  

**Note:** The following options were available prior to Drill 1.11, but have since been deprecated and replaced with the options described above:  

- Drill.exec.sort.external.spill.fs (Replaced by drill.exec.spill.fs)
- Drill.exec.sort.external.spill.directories (Replaced by drill.exec.spill.directories)
- Drill.exec.hashagg.spill.fs (Replaced by drill.exec.spill.fs)  


##Memory Allocation  

Drill evenly splits the available memory among all instances of the Sort, Hash Aggregate, and Hash Join operators. When a query is parallelized, the number of operators is multiplied, which reduces the amount of memory given to each instance of the operators during a query.  

**Memory Allocation Configuration Options**  

The `planner.memory.max_query_memory_per_node` and `planner.memory.percent_per_query` options set the amount of memory that Drill can allocate to a query on a node. Both options are enabled by default. Of these two options, Drill picks the setting that provides the most memory.  

- **planner.memory.max_query_memory_per_node**  
The `planner.memory.max_query_memory_per_node` option, set at 2 GB by default, is the minimum amount of memory available to Drill per query on a node. The default of 2 GB typically allows between two and three concurrent queries to run when the JVM is configured to use 8 GB of direct memory (default). When the memory requirement for Drill increases, the default of 2GB is constraining. You must increase the amount of memory for queries to complete, unless the setting for the planner.memory.percent_per_query option allows for Drill to use more memory.
- **planner.memory.percent_per_query**  
Alternatively, the `planner.memory.percent_per_query` option sets the memory as a percentage of the total direct memory. For example, if the allocation is set to 10%, and the total direct memory is 128 GB, each query gets approximately 13 GB.  

The percentage is calculated using the following formula:  

       (1 - non-managed allowance)/concurrency

The non-managed allowance is an assumed amount of system memory that non-managed operators will use. Non-managed operators do not spill to disk. The default non-managed allowance assumes 50% of the total system memory. And, the concurrency is the number of concurrent queries that may run. The default assumption is 10.

Based on the default assumptions, the default value of 5% is calculated as follows:  

       (1 - .50)/10 = 0.05  

This value is only used when throttling is disabled. Setting the value to 0 disables the option. You can increase or decrease the value, however you should set the percentage well below the JVM direct memory to account for the cases where Drill does not manage memory, such as for the less memory intensive operators.  

**Increasing the Available Memory**  

You can increase the amount of available memory to Drill using the ALTER SYSTEM|SESSION SET commands with the `planner.memory.max_query_memory_per_node` or `planner.memory.percent_per_query` options, as shown:  

       ALTER SYSTEM|SESSION SET `planner.memory.max_query_memory_per_node` = <new_value>
       //The default value is to 2147483648 bytes (2GB). 
       
       ALTER SYSTEM|SESSION SET `planner.memory.percent_per_query` = <new_value>
       //The default value is 0.05.  

##Disabling the Hash Operators  

You can disable the Hash Aggregate and Hash Join operators. When you disable these operators, Drill creates alternative query plans that use the Sort operator and the Streaming Aggregate or the Merge Join operator. 

Use the ALTER SYSTEM|SESSION SET commands with the following options to disable the Hash Aggregate and Hash Join operators. Typically, you set the options at the session level unless you want the setting to persist across all sessions. 

The following options control the hash-based operators:  

- **planner.enable_hashagg**  
Enables or disables hash aggregation; otherwise, Drill does a sort-based aggregation. This option is enabled by default. The default, and recommended, setting is true. Prior to Drill 1.11, the Hash Aggregate operator used an uncontrolled amount of memory (up to 10 GB), after which the operator ran out of memory. As of Drill 1.11, the Hash Aggregate operator can write to disk.
- **planner.enable_hashjoin**  
Enables or disables hash joins. This option is enabled by default. Drill assumes that a query will have adequate memory to complete and tries to use the fastest operations possible Drill 1.11, the Hash Join operator used an uncontrolled amount of memory (up to 10 GB), after which the operator ran out of memory. As of Drill 1.13, this operator can write to disk. This option is enabled by default.






 
  





