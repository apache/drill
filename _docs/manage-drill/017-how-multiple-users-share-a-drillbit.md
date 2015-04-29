---
title: "How Multiple Users Share a Drillbit"
parent: "Manage Drill"
---
To manage a cluster in which multiple users share a Drillbit, you configure Drill queuing and parallelization.

##Configuring Drill Query Queuing

Set [options in sys.options]({{site.baseurl}}/docs/configuration-options-introduction/) to enable and manage query queuing. Query queuing is off by default. There are two types of queues: large and small. You configure a maximum number of queries that each queue allows by configuring the following options in the `sys.options` table:

* exec.queue.large  
* exec.queue.small  

### Example Configuration

For example, you configure the queue reserved for large queries to hold a 5 query maximum. You configure the queue reserved for small queue to hold 20 queries. Users start to run queries, and Drill receives the following query requests in this order:

* Query A (blue): 1 billion records, Drill estimates 10 million rows will be processed  
* Query B (red): 2 billion records, Drill estimates 20 million rows will be processed  
* Query C: 1 billion records  
* Query D: 100 records

The exec.queue.threshold default is 30 million, which is the estimated rows to be processed by the query. Queries A and B are queued in the large queue, filling the queue to capacity. The estimated rows to be processed reaches the 30 million threshold. The query C request arrives and goes on the wait list, and then query D arrives. Query D is queued immediately in the small queue because of its small size, as shown in the following diagram: 

![drill queuing]({{ site.baseurl }}/docs/img/queuing.png)

This queuing technique tends to give many users running small queries a rapid response. Users running a large query might experience some delay until an earlier-received large query returns.

## Controlling Parallelization

By default, Drill parallelizes operations when number of records manipulated within a fragment reaches 100,000. When parallelization of operations is high, the cluster operates as fast as possible, which is fine for a single user. In a contentious multi-tenant situation, however, you need to reduce parallelization to levels based on user needs.

### Parallelization Configuration Procedure

To configure parallelization, configure the following options in the `sys.options` table:

* `planner.width.max.per.node`  
  The maximum degree of distribution of a query across cores and cluster nodes.
* `planner.width.max.per.query`  
  Same as max per node but applies to the query as executed by the entire cluster.

Configure the `planner.width.max.per.node` to achieve fine grained, absolute control over parallelization. <<For example, setting the `planner.width.max.per.query` to 60 will not accelerate Drill operations because overlapping does not occur when executing 60 queries at the same time.>>

### Example of Configuring Parallelization

For example, the default settings parallelize 70 percent of operations up to 1,000 cores. If you have 30 cores per node in a 10-node cluster, or 300 cores, parallelization occurs on approximately 210 cores. Consequently, a single user can get 70 percent usage from a cluster and no more due to the constraints configured by the `planner.width.max.per.query`.

A parallelizer in the Foreman transforms the physical plan into multiple phases. A complicated query can have multiple, major fragments. A default parallelization of 70 percent of operations allows some overlap of query phases. In the example, 210 <<for each core or major fragment to a maximum of 410>>.

<<Drill uses pipelines, blocking/nonblocking, memory is not fungible. CPU resources are fungible. There is contention for CPUs.>>

## Data Isolation

Data isolation is typically a requirement for multiple users on a Drill cluster. By using row/column level security permissions <<link to doc>> in views, you can achieve data isolation.









