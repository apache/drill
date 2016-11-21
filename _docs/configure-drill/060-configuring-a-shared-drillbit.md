---
title: "Configuring Resources for a Shared Drillbit"
date: 2016-11-21 22:42:10 UTC
parent: "Configuring a Multitenant Cluster"
---
To manage a cluster in which multiple users share a Drillbit, you configure Drill queuing and parallelization in addition to memory, as described in the previous section, ["Configuring Drill Memory"]({{site.baseurl}}/docs/configuring-drill-memory/).

##Configuring Query Queuing

Set [options in sys.options]({{site.baseurl}}/docs/configuration-options-introduction/) to enable and manage query queuing, which is turned off by default. There are two types of queues: large and small. You configure a maximum number of queries that each queue allows by configuring the following options in the `sys.options` table:

* exec.queue.large  
* exec.queue.small  
* exec.queue.threshold

The exec.queue.threshold sets the cost threshold for determining whether query is large or small based on complexity. Complex queries have higher thresholds. The default, 30,000,000, represents the estimated rows that a query will process. To serialize incoming queries, set the small queue at 0 and the threshold at 0.

For more information, see the section, ["Performance Tuning"](/docs/performance-tuning-introduction/).

## Configuring Parallelization

By default, Drill parallelizes operations when number of records manipulated within a fragment reaches 100,000. When parallelization of operations is high, the cluster operates as fast as possible, which is fine for a single user. In a contentious multi-tenant situation, however, you need to reduce parallelization to levels based on user needs.

### Parallelization Configuration Procedure

To configure parallelization, configure the following options in the `sys.options` table:

* `planner.width.max_per_node`  
  The maximum degree of distribution of a query across cores and cluster nodes.
* `planner.width.max_per_query`  
  Same as max per node but applies to the query as executed by the entire cluster.

### planner.width.max_per_node
Configure the `planner.width.max_per_node` to achieve fine grained, absolute control over parallelization. In this context *width* refers to fanout or distribution potential: the ability to run a query in parallel across the cores on a node and the nodes on a cluster. A physical plan consists of intermediate operations, known as query &quot;fragments,&quot; that run concurrently, yielding opportunities for parallelism above and below each exchange operator in the plan. An exchange operator represents a breakpoint in the execution flow where processing can be distributed. For example, a single-process scan of a file may flow into an exchange operator, followed by a multi-process aggregation fragment.

The maximum width per node defines the maximum degree of parallelism for any fragment of a query, but the setting applies at the level of a single node in the cluster. The *default* maximum degree of parallelism per node is calculated as follows, with the theoretical maximum automatically scaled back (and rounded down) so that only 70% of the actual available capacity is taken into account: number of active drillbits (typically one per node) * number of cores per node * 0.7

For example, on a single-node test system with 2 cores and hyper-threading enabled: 1 * 4 * 0.7 = 3

When you modify the default setting, you can supply any meaningful number. The system does not automatically scale down your setting.

### planner.width.max_per_query

The max_per_query value also sets the maximum degree of parallelism for any given stage of a query, but the setting applies to the query as executed by the whole cluster (multiple nodes). In effect, the actual maximum width per query is the *minimum of two values*: min((number of nodes * width.max_per_node), width.max_per_query)

For example, on a 4-node cluster where `width.max_per_node` is set to 6 and `width.max_per_query` is set to 30: min((4 * 6), 30) = 24

In this case, the effective maximum width per query is 24, not 30.

<!-- ??For example, setting the `planner.width.max.per.query` to 60 will not accelerate Drill operations because overlapping does not occur when executing 60 queries at the same time.??

### Example of Configuring Parallelization

For example, the default settings parallelize 70 percent of operations up to 1,000 cores. If you have 30 cores per node in a 10-node cluster, or 300 cores, parallelization occurs on approximately 210 cores. Consequently, a single user can get 70 percent usage from a cluster and no more due to the constraints configured by the `planner.width.max.per.query`.

A parallelizer in the Foreman transforms the physical plan into multiple phases. A complicated query can have multiple, major fragments. A default parallelization of 70 percent of operations allows some overlap of query phases. In the example, 210 ??for each core or major fragment to a maximum of 410??.

??Drill uses pipelines, blocking/nonblocking, memory is not fungible. CPU resources are fungible. There is contention for CPUs.?? -->

## Data Isolation

Tenants can share data on a cluster using Drill views and [impersonation]({{site.baseurl}}/docs/configuring-user-impersonation). 









