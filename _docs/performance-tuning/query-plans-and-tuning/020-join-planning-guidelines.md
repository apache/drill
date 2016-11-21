---
title: "Join Planning Guidelines"
date: 2016-11-21 22:28:42 UTC
parent: "Query Plans and Tuning"
--- 

Drill uses distributed and broadcast joins to join tables. You can modify configuration settings in Drill to control how Drill plans joins in a query.

## Distributed Joins
For a distributed join, both sides of the join are hash distributed using one of the hash-based distribution operators on the join key. See Operators. 

If there are multiple join keys from each table, Drill considers the two following types of plans:  
1. A plan where data is distributed on all keys.  
2. A plan where data is distributed on each individual key.  
 
For a merge join, Drill sorts both sides of the join after performing the hash distribution. Drill can distribute both sides of a hash join or merge join, but cannot do so for a nested loop join. 

## Broadcast Joins
In a broadcast join, all of the selected records of one file are broadcast to the file on all other nodes before the join is performed. The inner side of the join is broadcast while the outer side is kept as-is without any re-distribution. The estimated cardinality of the inner child must be below the planner.broadcast_threshold parameter in order to be eligible for broadcast.  Drill can use broadcast joins for hash, merge, and nested loop joins.
 
A broadcast join is useful when a large (fact) table is being joined to a relatively smaller (dimension) table. If the fact table is stored as many files in the distributed file system, instead of re-distributing the fact table over the network, it may be substantially cheaper to broadcast the inner side.  However, the broadcast sends the same data to all other nodes in the cluster.  Depending on the size of the cluster and the size of the data, it may not be the most efficient policy in some situations.
 
### Broadcast Join Options
You can increase the size and affinity for Drill to use broadcast joins with the ALTER SYSTEM or ALTER SESSION commands and options. Typically, you set the options at the session level unless you want the setting to persist across all sessions.

The following configuration options in Drill control broadcast join behavior:  

* **planner.broadcast_factor** 

     Controls the cost of doing a broadcast when performing a join.  The lower the setting, the cheaper it is to do a broadcast join compared to other types of distribution for a join, such as a hash distribution.  

     Default:1 Range: 0-1.7976931348623157e+308

* **planner.enable\_broadcast_join**  

     Changes the state of aggregation and join operators. The broadcast join can be used for hash join, merge join, and nested loop join. Use to join a large (fact) table to relatively smaller (dimension) tables.  

     Default: true 

* **planner.broadcast_threshold**  

    Threshold, in terms of a number of rows, that determines whether a broadcast join is chosen for a query. Regardless of the setting of the broadcast_join option (enabled or disabled), a broadcast join is not chosen unless the right side of the join is estimated to contain fewer rows than this threshold. The intent of this option is to avoid broadcasting too many rows for join purposes. Broadcasting involves sending data across nodes and is a network-intensive operation. (The "right side" of the join, which may itself be a join or simply a table, is determined by cost-based optimizations and heuristics during physical planning.)  
    
    Default: 10000000 Range: 0-2147483647
