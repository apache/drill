---
title: "How Multiple Users Share a Drillbit"
parent: "Manage Drill"
---
To manage a cluster in which multiple users share a Drillbit, you configure Drill queuing and parallelization.

##Configuring Drill Query Queuing

Set [Options in sys.options]() to enable and manage query queuing. Query queuing is off by default. There are two types of queues: large and small. You configure a maximum number of queries that each queue allows by configuring the following options in the `sys.options` table:

* exec.queue.large  
* exec.queue.small  

For example, you have the queue reserved for large queries configured for 5 queries and the queue reserved for small queue for 20 queries. Drill receives the following query requests in this order:

* Query A (blue): 1 billion records, Drill estimates 10 million rows will be processed  
* Query B (red): 2 billion records, Drill estimates 20 million rows will be processed  
* Query C: 1 billion records  
* Query D: 100 records

The exec.queue.threshold default is 30 million, which is the estimated rows to be processed by the query. Queries A and B are queued in the large queue, filling the queue to capacity. The estimated rows to be processed reaches the 30 million threshold. The query C request arrives and goes on the wait list, and then query D arrives. Query D is queued immediately in the small queue because of its small size, as shown in the following diagram: 

![drill queuing]({{ site.baseurl }}/docs/img/queuing.png)

This queuing technique tends to give many users running small queries a rapid response. Users running a large query might experience some delay until an earlier-received large query returns.

## Parallelization

The Drill default sets parallelization high and the cluster operates as fast as possible, which is fine for a single user. In a contentious multi-tenant situation, you need to reduce parallelization by configuring the following options in the `sys.options` table:

* planner.width.max.per.node  
* planner.width.max.per queue










