---
title: "Sort-Based and Hash-Based Memory-Constrained Operators"
date: 2018-12-19
parent: "Query Plans and Tuning"
---  

Drill supports the following memory-intensive operators, which can temporarily spill data to disk if they run out of memory:  

- External Sort
- Hash-Join (with semi-join functionality in Drill 1.15 and later)
- Hash-Aggregate

Drill only uses the External Sort operator to sort data. Drill uses the Hash-Aggregate operator to aggregate data. Alternatively, Drill can sort the data and then use the (lightweight) Streaming-Aggregate operator to aggregate data.
Drill uses the Hash-Join operator to join data. Drill 1.15 introduces semi-join functionality inside the Hash-Join operator to improve query performance. Semi-joins remove the distinct processing below the Hash-Join and eliminate the overhead incurred from using a Hash Aggregate. Prior to Drill 1.15 (or when [semi-join functionality is disabled]({{site.baseurl}}/docs/sort-based-and-hash-based-memory-constrained-operators/#disabling-the-hash-operators)), Drill uses a Distinct Hash Aggregate to implement the functionality of a semi-join. Alternatively, Drill can use the Nested-Loop-Join or sort the data and then use the (lightweight) Merge-Join. Drill typically uses Hash operators for joining and aggregation, as they perform better than the Sort operator (Hash - O(N) vs. Sort - O(N * log(N))). However, if you disable the Hash operators, or the data is already sorted, Drill uses the alternative methods previously described.

The memory configuration in Drill is specified as the memory limit per-query, per-node. The allocated memory is equally divided among all instances of the spillable operators (per query on each node). The number of instances is the number of spillable operators in the query plan multiplied by the maximal degree of parallelism. The maximal degree of parallelism is the number of minor fragments required to perform the work for each instance of a spillable operator. When an instance of a spillable operator must process more data than it can hold, the operator temporarily spills some of the data to a directory on disk to complete its work.  

##Spill to Disk  

Spilling to disk prevents queries that use memory intensive operations from failing with out-of-memory errors. The Spill to Disk feature enables the spillable operators to automatically spill (write) excess data (as files) to a temporary directory on disk when the memory requirements for the operators exceed the set memory limit. Queries run uninterrupted while the operators perform the spill operations in the background.

When the spillable operators finish processing data in memory, they read the spilled data back from disk and then finish processing the data. The operators clean up their data (files) from the temporary spill location after they finish processing the data. 

Ideally, you want to allocate enough memory for Drill to perform all operations in memory. When data spills to disk, you will not see any difference in terms of how queries run; however, spilling to disk can impact performance due to the additional I/O required to write data to disk and read the data back. For more information, see [Memory Allocation]({{site.baseurl}}/docs/sort-based-and-hash-based-memory-constrained-operators/#memory-allocation). 

**Note:** Drill 1.14 and later supports spilling to disk for the Hash Join, Hash Aggregate, and Sort operators. Drill 1.11, 1.12, and 1.13 supports spilling to disk for the Hash Aggregate and Sort operators. Releases of Drill prior to 1.11 only support spilling to disk for the Sort operator.  

**Spill Locations** 

Spillable operators write data to a temporary work area on disk when they cannot process all of the data in memory. The default location of the temporary work area is `/tmp/drill/spill` on the local file system. 

The `/tmp/drill/spill` directory should suffice for small workloads or examples; however, you should redirect the default spill location to a location with enough disk space to support spilling for large workloads.

**Note:** Spilled data may require more space than the table referenced in the query that is spilling the data. For example, when the underlying table is compressed (Parquet), or when the operator received data joined from multiple tables.

When you configure the spill location, you can specify a single directory or a list of directories into which the spillable operators spill data.  

**Configuring Spill to Disk**  

The `drill-override.conf` file, located in the `/conf` directory, contains options that set the spill locations for the spillable operators. An administrator can change the file system and directories into which the operators spill data. Refer to the `drill-override-example.conf` file included in the `/conf` directory for examples. 

The following list describes the spill to disk configuration options:  

- **drill.exec.spill.fs**  
Introduced in Drill 1.11. The default file system on the local machine into which the spillable operators spill data. You can configure this option so that data spills into a distributed file system, such as hdfs. For example, "hdfs:///". The default setting is "file:///".  

- **drill.exec.spill.directories**  
Introduced in Drill 1.11. The list of directories into which the spillable operators spill data. The list must be an array with directories separated by a comma, for example ["/fs1/drill/spill" , "/fs2/drill/spill" , "/fs3/drill/spill"]. The default setting is ["/tmp/drill/spill"].  

**Note:** The following options were available prior to Drill 1.11, but have since been deprecated and replaced with the options described above:  

- Drill.exec.sort.external.spill.fs (Replaced by drill.exec.spill.fs)
- Drill.exec.sort.external.spill.directories (Replaced by drill.exec.spill.directories)
- Drill.exec.hashagg.spill.fs (Replaced by drill.exec.spill.fs)  


##Memory Allocation  

Drill evenly splits the available memory among all instances of the spillable operators. When a query is parallelized, the number of operators is multiplied, which reduces the amount of memory given to each instance of the operators during a query. To see the difference in memory consumption between the operators, you can run a query and then view the query profile in the Drill Web UI. Optionally, you can disable the Hash operators, which forces Drill to use the Merge-Join and Streaming-Aggregate operators.  

**Memory Allocation Configuration Options**  

The `planner.memory.max_query_memory_per_node` and `planner.memory.percent_per_query` options set the amount of memory that Drill can allocate to a query on a node. Both options are enabled by default. Of these two options, Drill picks the setting that provides the most memory.  

- **planner.memory.max\_query\_memory\_per_node**  
The `planner.memory.max_query_memory_per_node` option is the minimum amount of memory available to Drill per query on a node. The default of 2 GB typically allows between two and three concurrent queries to run when the JVM is configured to use 8 GB of direct memory (default). When the memory requirement for Drill increases, the default of 2 GB is constraining. You must increase the amount of memory for queries to complete, unless the setting for the `planner.memory.percent_per_query` option allows for Drill to use more memory.  

- **planner.memory.percent\_per_query**  
Alternatively, the `planner.memory.percent_per_query` option sets the memory as a percentage of the total direct memory. The default is 5%. This value is only used when throttling is disabled. Setting the value to 0 disables the option. You can increase or decrease the value; however, you should set the percentage well below the JVM direct memory to account for the cases where Drill does not manage memory, such as for the less memory intensive operators. 

       - The percentage is calculated using the following formula:    

              (1 - non-managed allowance)/concurrency  

       - The non-managed allowance is an assumed amount of system memory that non-managed operators will use. Non-managed operators do not spill to disk. The conservative assumption for the non-managed allowance is 50% of the total system memory. Concurrency is the number of concurrent queries that may run. The default assumption is 10 concurrent queries.  

       - Based on the default assumptions, the default value of 5% is calculated, as shown:  

            (1 - .50)/10 = 0.05  


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
Enables or disables hash aggregation; otherwise, Drill does a sort-based aggregation. This option is enabled by default. The default, and recommended, setting is true. Prior to Drill 1.11, the Hash Aggregate operator used an uncontrolled amount of memory (up to 10 GB), after which the operator ran out of memory. As of Drill 1.11, the Hash Aggregate operator can spill to disk.  

- **planner.enable_hashjoin**  
Enables or disables hash joins. This option is enabled by default. Drill assumes that a query will have adequate memory to complete and tries to use the fastest operations possible. Prior to Drill 1.14, the Hash-Join operator used an uncontrolled amount of memory (up to 10 GB), after which the operator ran out of memory. As of Drill 1.14, this operator can spill to disk. This option is enabled by default.    

- **planner.enable_semijoin**  
Enables or disables semi-joins. This option is enabled by default and only works when the `planner.enable_hashjoin` option is also enabled. When enabled, Drill uses semi-joins to remove the distinct processing below the Hash Join and sets the semi-join flag in the Hash Join flag, as shown in the following example:  

###Example: Query Plan with and without Semi-Join

**Semi-Join Disabled**   
In the following query plan, you can see the HashAgg before the HashJoin. In the HashJoin flag, you can see that semi-join flag is set to false, indicating that a semi-join was not used.  

	EXPLAIN PLAN FOR SELECT employee_id, full_name FROM cp.`employee.json` WHERE employee_id IN (SELECT employee_id FROM cp.`employee.json`);  

	+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
	|                                       text                                       |                                                            
	+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
	| 00-00    Screen
	00-01      Project(employee_id=[$0], full_name=[$1])
	00-02        Project(employee_id=[$0], full_name=[$1])
	00-03          HashJoin(condition=[=($0, $2)], joinType=[inner], semi-join: =[false])
	00-05            Scan(table=[[cp, employee.json]], groupscan=[EasyGroupScan [selectionRoot=classpath:/employee.json, numFiles=1, columns=[`employee_id`, `full_name`], files=[classpath:/employee.json]]])
	00-04            Project(employee_id0=[$0])
	00-06              HashAgg(group=[{0}])
	00-07                Scan(table=[[cp, employee.json]], groupscan=[EasyGroupScan [selectionRoot=classpath:/employee.json, numFiles=1, columns=[`employee_id`], files=[classpath:/employee.json]]])
	planner.enable_semijoin  

**Semi-Join Enabled**   
In the following query plan, you can see that the HashAgg is absent. In the HashJoin flag, you can see that semi-join flag is set to true, indicating that a semi-join was used. Using the semi-join optimizes the query by reducing the amount of processing that Drill must perform on data.   

	EXPLAIN PLAN FOR SELECT employee_id, full_name FROM cp.`employee.json` WHERE employee_id IN (SELECT employee_id FROM cp.`employee.json`);
	--------------------------------------------------------------------------------+
	|                                       text                                       |                                     +----------------------------------------------------------------------------------+
	| 00-00    Screen
	00-01      Project(employee_id=[$0], full_name=[$1])
	00-02        HashJoin(condition=[=($0, $2)], joinType=[inner], semi-join: =[true])
	00-04          Scan(table=[[cp, employee.json]], groupscan=[EasyGroupScan [selectionRoot=classpath:/employee.json, numFiles=1, columns=[`employee_id`, `full_name`], files=[classpath:/employee.json]]])
	00-03          Scan(table=[[cp, employee.json]], groupscan=[EasyGroupScan [selectionRoot=classpath:/employee.json, numFiles=1, columns=[`employee_id`], files=[classpath:/employee.json]]])



 






 
  





