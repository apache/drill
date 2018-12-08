---
title: "Throttling"
date: 2018-12-08
parent: "Query Plans and Tuning"
--- 

Drill 1.12 introduces throttling. Throttling limits the number of concurrent queries that run to prevent queries from failing with out-of-memory errors. When you enable throttling, you configure the number of concurrent queries that can run and the resource requirements for each query. Drill calculates the amount of memory to assign per query per node.

If throttling is disabled, you most likely need to increase the amount of memory assigned to the planner.memory.max_query_memory_per_node option based on the amount of direct memory that Drill will allocate to the [Sort and Hash Aggregate operators]({{site.baseurl}}/docs/sort-based-and-hash-based-memory-constrained-operators/) during each query on a node. Drill must decide how much memory to assign to each of these operators, without knowing how many concurrent queries might run. If Drill does not give enough memory to the Sort and Hash Aggregate operators, queries fail. Using the throttling feature prevents this from happening.
   

## Configuring Throttling  

You can enable and configure query queuing using system options that you set in the Drill Web UI or through SQL statements. The examples in this document use SQL statements. 

**Note:** The memory for the small and large query queues is calculated as:  

       Memory unit = small_queue + (large queue * memory_ratio)
       Total memory available = total_direct_mem * (1 - memory_reserve_ratio)
       SQueue memory allocation = total_mem_available/memory_unit
       LQueue memory allocation = SQueue_memory_allocation * memory_ratio  

### Enable Throttling  

You can turn throttling on and off using the exec.queue.enable option, as shown:

       ALTER SYSTEM SET `exec.queue.enable` = true

The main page of the Drill Web UI shows the current queue configuration when throttling is enabled.  

![](https://i.imgur.com/qfzE2pR.png)  

### Configuring Queue Sizes  

Determine how many queries run well concurrently on the system. Set the queue limits to the number of queries that can run concurrently. Be conservative when setting the numbers; start small and work upward. The larger the number, the smaller the amount of memory given to each query, which can force spilling and eventually cause out-of-memory errors. 

You can set the queue sizes using the exec.queue.large and exec.queue.small options, as shown:

       ALTER SYSTEM SET `exec.queue.large` = 4
       ALTER SYSTEM SET `exec.queue.small` = 10

**Note:** Values used here are examples. Evaluate your queries to determine the best queue sizes for your query load.  

### Configuring Queue Timeout  

Queue timeout defines the amount of time a query waits to run. The default setting is five minutes, set in milliseconds. You can set the query timeout using the exec.queue.timeout_millis option, as shown:

       ALTER SYSTEM SET `exec.queue.timeout_millis` = 180000

**Note:** Calculate the number of milliseconds:

       Ms = 3 mins. * 60 seconds/minute * 1000 ms/sec = 180,000

Once a queue has the maximum number of queries running, subsequent queries enter the queue and wait for a free slot. For example, if the small queue size is set to ten, ten small queries can run. The eleventh query waits until one of the first ten queries completes.  

### Configuring the Queue Threshold
The queue threshold tells Drill how to sort queries into the small and large queues. You can set the queue threshold through the exec.queue.threshold option, as shown:

       ALTER SYSTEM SET `exec.queue.threshold` = 123,000,000

The value that you set represents the total query cost, as computed by the planner. The value does not directly map to physical numbers, however it is derived from row count, CPU, network cost, and so on. 

Before you set the value, experiment by running queries that you consider small and large, then look at the total cost per query in the Drill Web UI, and pick a number in between when setting the threshold option.  

![](https://i.imgur.com/5kpdkCy.png)  

### Configuring the Memory Ratio
By default, Drill assumes that large queries use 10x the memory of small queries. You can change the memory ration using the exec.queue.memory_ratio option, as shown:

       ALTER SYSTEM SET `exec.queue.memory_ratio` = 8

The default setting is 10, however if by experimentation you find that another number would be better, change the ratio to meet the needs of your queries.  

### Configuring the Memory Reserve Ratio
Only the Sort and Hash Aggregate operators observer a memory limit and [spill to disk]({{site.baseurl}}/docs/sort-based-and-hash-based-memory-constrained-operators/#spill-to-disk). Other operators are not managed and the amount of memory needed for these other operators varies based on your specific queries. To account for these operators, you can set a memory reserve. 
The default reserve is 20% (0.2), but join-heavy workloads may need a larger value such as 50% or more.

You can change the memory reserve ration using the exec.queue.memory_reserve_ratio option, as shown:

       ALTER SYSTEM SET `exec.queue.memory_reserve_ratio` = 0.5  

## Tuning  

To use query profiles to determine the proper parameters: 

- Set the query sizes conservatively to ensure queries succeed.  
- Experiment to adjust the size threshold by observing actual costs for typical queries.  
- Tweak the memory settings if problems occur such as out-of-memory due to joins and so on.








   



