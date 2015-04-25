---
title: "Planning and Execution Options"
parent: "Configuration Options"
---
You can set Drill query planning and execution options per cluster, at the
system or session level. Options set at the session level only apply to
queries that you run during the current Drill connection. Options set at the
system level affect the entire system and persist between restarts. Session
level settings override system level settings.

You can run the following query to see a list of the system and session
planning and execution options:

    SELECT name FROM sys.options WHERE type in (SYSTEM, SESSION);

## Configuring Planning and Execution Options

Use the ALTER SYSTEM or ALTER SESSION commands to set options. Typically,
you set the options at the session level unless you want the setting to
persist across all sessions.

The summary of system options lists default values. The following descriptions provide more detail on some of these options:

### exec.min_hash_table_size

The default starting size for hash tables. Increasing this size is useful for very large aggregations or joins when you have large amounts of memory for Drill to use. Drill can spend a lot of time resizing the hash table as it finds new data. If you have large data sets, you can increase this hash table size to increase performance.

### planner.add_producer_consumer

This option enables or disables a secondary reading thread that works out of band of the rest of the scanning fragment to prefetch data from disk. If you interact with a certain type of storage medium that is slow or does not prefetch much data, this option tells Drill to add a producer consumer reading thread to the operation. Drill can then assign one thread that focuses on a single reading fragment. If Drill is using memory, you can disable this option to get better performance. If Drill is using disk space, you should enable this option and set a reasonable queue size for the planner.producer_consumer_queue_size option.

### planner.broadcast_threshold

Threshold, in terms of a number of rows, that determines whether a broadcast join is chosen for a query. Regardless of the setting of the broadcast_join option (enabled or disabled), a broadcast join is not chosen unless the right side of the join is estimated to contain fewer rows than this threshold. The intent of this option is to avoid broadcasting too many rows for join purposes. Broadcasting involves sending data across nodes and is a network-intensive operation. (The &quot;right side&quot; of the join, which may itself be a join or simply a table, is determined by cost-based optimizations and heuristics during physical planning.)

### planner.enable_broadcast_join, planner.enable_hashagg, planner.enable_hashjoin, planner.enable_mergejoin, planner.enable_multiphase_agg, planner.enable_streamagg

These options enable or disable specific aggregation and join operators for queries. These operators are all enabled by default and in general should not be disabled.</p><p>Hash aggregation and hash join are hash-based operations. Streaming aggregation and merge join are sort-based operations. Both hash-based and sort-based operations consume memory; however, currently, hash-based operations do not spill to disk as needed, but the sort-based operations do. If large hash operations do not fit in memory on your system, you may need to disable these operations. Queries will continue to run, using alternative plans.

### planner.producer_consumer_queue_size

Determines how much data to prefetch from disk (in record batches) out of band of query execution. The larger the queue size, the greater the amount of memory that the queue and overall query execution consumes.

### planner.width.max_per_node

In this context *width* refers to fanout or distribution potential: the ability to run a query in parallel across the cores on a node and the nodes on a cluster. A physical plan consists of intermediate operations, known as query &quot;fragments,&quot; that run concurrently, yielding opportunities for parallelism above and below each exchange operator in the plan. An exchange operator represents a breakpoint in the execution flow where processing can be distributed. For example, a single-process scan of a file may flow into an exchange operator, followed by a multi-process aggregation fragment.

The maximum width per node defines the maximum degree of parallelism for any fragment of a query, but the setting applies at the level of a single node in the cluster. The *default* maximum degree of parallelism per node is calculated as follows, with the theoretical maximum automatically scaled back (and rounded down) so that only 70% of the actual available capacity is taken into account: number of active drillbits (typically one per node) * number of cores per node * 0.7

For example, on a single-node test system with 2 cores and hyper-threading enabled: 1 * 4 * 0.7 = 3

When you modify the default setting, you can supply any meaningful number. The system does not automatically scale down your setting.

### planner.width.max_per_query

The max_per_query value also sets the maximum degree of parallelism for any given stage of a query, but the setting applies to the query as executed by the whole cluster (multiple nodes). In effect, the actual maximum width per query is the *minimum of two values*: min((number of nodes * width.max_per_node), width.max_per_query)

For example, on a 4-node cluster where `width.max_per_node` is set to 6 and `width.max_per_query` is set to 30: min((4 * 6), 30) = 24

In this case, the effective maximum width per query is 24, not 30.