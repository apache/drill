---
title: "Enabling Query Queuing"
date: 2016-11-21 22:28:44 UTC
parent: "Query Plans and Tuning"
--- 

Drill runs all queries concurrently by default. However, Drill performance increases when a small number of queries run concurrently. You can enable query queues to limit the maximum number of queries that run concurrently. Splitting large queries into multiple small queries and enabling query queuing improves query performance.
 
When you enable query queuing, you configure large and small queues. Drill determines which queue to route a query to at runtime based on the size of the query. Drill can quickly complete the queries and then continue on to the next set of queries.

## Example Configuration  

For example, you configure the queue reserved for large queries for a 5-query maximum. You configure the queue reserved for small queries for 20 queries. Users start to run queries, and Drill receives the following query requests in this order:  

* Query A (blue): 1 billion records, Drill estimates 10 million rows will be processed
* Query B (red): 2 billion records, Drill estimates 20 million rows will be processed
* Query C: 1 billion records
* Query D: 100 records
 
The exec.queue.threshold default is 30 million, which is the estimated rows to be processed by the query. Queries A and B are queued in the large queue. The estimated rows to be processed reaches the 30 million threshold, filling the queue to capacity. The query C request arrives and goes on the wait list, and then query D arrives. Query D is queued immediately in the small queue because of its small size, as shown in the following diagram:

![]({{ site.baseurl }}/docs/img/query_queuing.png)  

The Drill queuing configuration in this example tends to give many users running small queries a rapid response. Users running a large query might experience some delay until an earlier-received large query returns, freeing space in the large queue to process queries that are waiting.

Use the ALTER SYSTEM or ALTER SESSION commands with the options below to enable query queuing and set the maximum number of queries that each queue allows. Typically, you set the options at the session level unless you want the setting to persist across all sessions.


* **exec.queue.enable**  
    Changes the state of query queues to control the number of queries that run simultaneously. When disabled, there is no limit on the number of concurrent queries.  
    Default: false

* **exec.queue.large**  
    Sets the number of large queries that can run concurrently in the cluster.  
    Range: 0-1000. Default: 10

* **exec.queue.small**  
    Sets the number of small queries that can run concurrently in the cluster. Range: 0-1001.  
    Range: 0 - 1073741824 Default: 100

* **exec.queue.threshold**  
    Sets the cost threshold, which depends on the complexity of the queries in queue, for determining whether a query is large or small. Complex queries have higher thresholds.  
    Range: 0-9223372036854775807 Default: 30000000

* **exec.queue.timeout_millis**  
    Indicates how long a query can wait in queue before the query fails.  
    Range: 0-9223372036854775807 Default: 300000


