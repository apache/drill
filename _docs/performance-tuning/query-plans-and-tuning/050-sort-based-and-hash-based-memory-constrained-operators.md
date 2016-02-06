---
title: "Sort-Based and Hash-Based Memory-Constrained Operators"
date: 2016-02-06 00:18:13 UTC
parent: "Query Plans and Tuning"
--- 

Drill uses hash-based and sort-based operators depending on the query characteristics. Hash aggregation and hash join are hash-based operations. Streaming aggregation and merge join are sort-based operations. Both hash-based and sort-based operations consume memory, however the hash aggregate and hash join operators are the fastest and most memory intensive operators.
 
Currently, hash-based operations do not spill to disk as needed, but the sort-based operations do. When Drill plans a sort-based query, it evaluates the size of available memory multiplied by a configurable reduction constant (for parallelization purposes) and then limits the sort-based operations to the maximum of this amount of memory.

If the hash-based operators run out of memory during execution, the query fails. If large hash operations do not fit in memory on your system, you can disable these operations. When disabled, Drill creates alternative plans that allow spilling to disk.

You can also modify the minimum hash table size, increasing the size for very large aggregations or joins when you have large amounts of memory for Drill to use. If you have large data sets, you can increase this hash table size to improve performance.
 
Use the ALTER SYSTEM or ALTER SESSION commands with the options in the table below to disable the hash aggregate and hash join operators, modify the hash table size, disable memory estimation, or set the estimated maximum amount of memory for a query. Typically, you set the options at the session level unless you want the setting to persist across all sessions.

The following options control the hash-based operators:

* **planner.enable_hashagg**  
    Enable hash aggregation; otherwise, Drill does a sort-based aggregation. Does not write to disk. Enable is recommended. Default: true

* **planner.enable_hashjoin**  
    Enable the memory hungry hash join. Drill assumes that a query will have adequate memory to complete and tries to use the fastest operations possible to complete the planned inner, left, right, or full outer joins using a hash table. Does not write to disk. Disabling hash join allows Drill to manage arbitrarily large data in a small memory footprint. Default: true

* **exec.min_hash_table_size**  
    Starting size for hash tables. Increase according to available memory to improve performance.  
    Default: 65536 Range: 0 - 1073741824

* **exec.max\_hash\_table_size**  
    Ending size for hash tables.  
    Default: 1073741824 Range: 0 - 1073741824

* **planner.memory.enable\_memory_estimation**  
    Toggles the state of memory estimation and re-planning of the query. When enabled, Drill conservatively estimates memory requirements and typically excludes memory-constrained operators from the plan and negatively impacts performance.  
    Default: false


* **planner.memory.max\_query\_memory\_per_node**  
    Sets the maximum amount of direct memory allocated to the sort operator in each query on a node. If a query plan contains multiple sort operators, they all share this memory. If you encounter memory issues when running queries with sort operators, increase the value of this option.  
    Default: 2147483648 

