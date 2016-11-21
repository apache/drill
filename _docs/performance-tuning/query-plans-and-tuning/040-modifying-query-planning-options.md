---
title: "Modifying Query Planning Options"
date: 2016-11-21 22:28:44 UTC
parent: "Query Plans and Tuning"
--- 

Planner options affect how Drill plans a query. You can use the ALTER SYSTEM|SESSION commands to modify certain planning options to optimize query plans and improve performance.  Typically, you modify options at the session level. See [ALTER SESSION]({{ site.baseurl }}/docs/alter-session/) for details on how to run the command.
 
The following planning options affect query planning and performance:

* **planner.width.max\_per_node**  
     Configure this option to achieve fine grained, absolute control over parallelization.

     In this context width refers to fan out or distribution potential: the ability to run a query in parallel across the cores on a node and the nodes on a cluster. A physical plan consists of intermediate operations, known as query "fragments," that run concurrently, yielding opportunities for parallelism above and below each exchange operator in the plan. An exchange operator represents a breakpoint in the execution flow where processing can be distributed. For example, a single-process scan of a file may flow into an exchange operator, followed by a multi-process aggregation fragment.
 
     The maximum width per node defines the maximum degree of parallelism for any fragment of a query, but the setting applies at the level of a single node in the cluster. The default maximum degree of parallelism per node is calculated as follows, with the theoretical maximum automatically scaled back (and rounded down) so that only 70% of the actual available capacity is taken into account: number of active drillbits (typically one per node) * number of cores per node * 0.7
 
     For example, on a single-node test system with 2 cores and hyper-threading enabled: 1 * 4 * 0.7 = 3.
     When you modify the default setting, you can supply any meaningful number. The system does not automatically scale down your setting.  

* **planner.width\_max\_per_query**  
     Default is 1000. The maximum number of threads than can run in parallel for a query across all nodes. Only change this setting when Drill over-parallelizes on very large clusters.
 
* **planner.slice_target**  
     Default is 100000. The minimum number of estimated records to work with in a major fragment before applying additional parallelization.
 
* **planner.broadcast_threshold**  
     Default is 10000000. The maximum number of records allowed to be broadcast as part of a join. After one million records, Drill reshuffles data rather than doing a broadcast to one side of the join. To improve performance you can increase this number, especially on 10GB Ethernet clusters.
 


