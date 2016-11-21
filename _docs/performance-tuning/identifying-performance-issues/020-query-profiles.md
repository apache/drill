---
title: "Query Profiles"
date: 2016-11-21 22:28:41 UTC
parent: "Identifying Performance Issues"
---

A profile is a summary of metrics collected for each query that Drill executes. Query profiles provide information that you can use to monitor and analyze query performance. Drill creates a query profile from major, minor, operator, and input stream profiles. Each major fragment profile consists of a list of minor fragment profiles. Each minor fragment profile consists of a list of operator profiles. An operator profile consists of a list of input stream profiles. 

You can view aggregate statistics across profile lists in the Profile tab of the Drill Web Console at `<drill_node_ip_address>:8047`. You can modify and resubmit queries, or cancel queries. For debugging purposes, you can use profiles in conjunction with Drill logs. See Log and Debug.
 
Metrics in a query profile are associated with a coordinate system of IDs. Drill uses a coordinate system comprised of query, fragment, and operator identifiers to track query execution activities and resources. Drill assigns a unique QueryID to each query received and then assigns IDs to each fragment and operator that executes the query.
 
**Example IDs**

QueryID: 2aa98add-15b3-e155-5669-603c03bfde86
 
Fragment and operator IDs:  

![]({{ site.baseurl }}/docs/img/xx-xx-xx.png)  

## Viewing a Query Profile  

When you select the Profiles tab in the Drill Web Console at `<drill_node_ip_address>:8047`, you see a list of the last 100 queries than have run or that are currently running in the cluster.  

![]({{ site.baseurl }}/docs/img/list_queries.png)


You can click on any query to see its profile.  

![]({{ site.baseurl }}/docs/img/query_profile.png)  

When you select a profile, notice that the URL in the address bar contains the QueryID. For example, 2aa98add-15b3-e155-5669-603c03bfde86 in the following URL:

       http://<drill_node>:8047/profiles/2aa98add-15b3-e155-5669-603c03bfde86
 
The Query Profile section in the Query profile summarizes a few key details about the query, including: 
 
 * The state of the query, either running, completed, or failed.  
 * The node operating as the Foreman; the Drillbit that receives a query from the client or application becomes the Foreman and drives the entire query. 
 * The total number of minor fragments required to execute the query

If you scroll down, you can see the Fragment Profiles and Operator Profiles sections. 
 
## Fragment Profiles  

Fragment profiles section provides an overview table, and a major fragment block for each major fragment that executed the query. Each row in the Overview table provides the number of minor fragments that Drill parallelized from each major fragment, as well as aggregate time and memory metrics for the minor fragments.  

![]({{ site.baseurl }}/docs/img/frag_profile.png)  

See Major Fragment Profiles Table for column descriptions.
 
When you look at the fragment profiles, you may notice that some major fragments were parallelized into substantially fewer minor fragments, but happen to have the highest runtime.  Or, you may notice certain minor fragments have a higher peak memory than others. When you notice these variations in execution, you can delve deeper into the profile by looking at the major fragment blocks.
 
Below the Overview table are major fragment blocks. Each of these blocks corresponds to a row in the Overview table. You can expand the blocks to see metrics for all of the minor fragments that were parallelized from each major fragment, including the host on which each minor fragment ran. Each row in the major fragment table presents the fragment state, time metrics, memory metrics, and aggregate input metrics of each minor fragment.  

![]({{ site.baseurl }}/docs/img/maj_frag_block.png)  

When looking at the minor fragment metrics, verify the state of the fragment. A fragment can have a “failed” state which could indicate an issue on the host. If the query itself fails, an operator may have run out of memory. If fragments running on a particular node are under performing, there may be multi-tenancy issues that you can address.
 
You can also see a graph that illustrates the activity of major and minor fragments for the duration of the query.  

![]({{ site.baseurl }}/docs/img/graph_1.png)  

If you see “stair steps” in the graph, this indicates that the execution work of the fragments is not distributed evenly. Stair steps in the graph typically occur for non-local reads on data. To address this issue, you can increase data replication, rewrite the data, or file a JIRA to get help with the issue.
 
This graph correlates with the visualized plan graph in the Visualized Plan tab. Each color in the graph corresponds to the activity of one major fragment.  

![]({{ site.baseurl }}/docs/img/vis_graph.png)  

The visualized plan illustrates color-coded major fragments divided and labeled with the names of the operators used to complete each phase of the query. Exchange operators separate each major fragment. These operators represent a point where Drill can execute operations below them in parallel.  

## Operator Profiles  

Operator profiles describe each operator that performed relational operations during query execution. The Operator Profiles section provides an Overview table of the aggregate time and memory metrics for each operator within a major fragment.  

![]({{ site.baseurl }}/docs/img/operator_table.png)  

See Operator Profiles Table for column descriptions.
 
Identify the operations that consume a majority of time and memory. You can potentially modify options related to the specific operators to improve performance.
 
Below the Overview table are operator blocks, which you can expand to see metrics for each operator. Each of these blocks corresponds to a row in the Overview table. Each row in the Operator block presents time and memory metrics, as well as aggregate input metrics for each minor fragment.  

![]({{ site.baseurl }}/docs/img/operator_block.png)  

See Operator Block for column descriptions.
 
Drill uses batches of records as a basic unit of work. The batches are pipelined between each operation.  Record batches are no larger than 64k records. While the target size of one record batch is generally 256k, they can scale to many megabytes depending on the query plan and the width of the records.

The Max Records number for each minor fragment should be almost equivalent. If one, or a very small number of minor fragments, perform the majority of the work, there may be data skew. To address data skew, you may need change settings related to table joins or partition data to balance the work.  

### Data Skew Example
The following query was run against TPC-DS data:

       0: jdbc:drill:zk=local> select ss_customer_sk, count(*) as cnt from store_sales where ss_customer_sk is null or ss_customer_sk in (1, 2, 3, 4, 5) group by ss_customer_sk;
       +-----------------+---------+
       | ss_customer_sk  |   cnt   |
       +-----------------+---------+
       | null            | 129752  |
       | 5               | 47      |
       | 1               | 9       |
       | 2               | 43      |
       | 4               | 10      |
       | 3               | 11      |
       +-----------------+---------+
       6 rows selected
 
In the result set, notice that the 'null' group has 129752 values while others have roughly similar values.  

Looking at the operator profile for the hash aggregate in major fragment 00, you can see that out of 8 minor fragments, only minor fragment 1 is processing a substantially larger number of records when compared to the other minor fragments.  

![]({{ site.baseurl }}/docs/img/data_skew.png)  

In this example, there is inherent skew present in the data. Other types of skew may not strictly be data dependent, but can be introduced by a sub-optimal hash function or other issues in the product. In either case, examining the query profile helps understand why a query is slow. In the first scenario, it may be possible to run separate queries for the skewed and non-skewed values. In the second scenario, it is better to seek technical support.  

## Physical Plan View  

The physical plan view provides statistics about the actual cost of the query operations in terms of memory, I/O, and CPU processing. You can use this profile to identify which operations consumed the majority of the resources during a query, modify the physical plan to address the cost-intensive operations, and submit the updated plan back to Drill. See [Costing Information]({{ site.baseurl }}/docs/explain/#costing-information).  

![]({{ site.baseurl }}/docs/img/phys_plan_profile.png)  

## Canceling a Query  

You may want to cancel a query if it hangs or causes performance bottlenecks. You can cancel a query in the Profile tab of the Drill Web Console.
 
To cancel a query from the Drill Web Console, complete the following steps:  

1. Navigate to the Drill Web Console at `<drill_node_ip_address>:8047`.
The Drill node from which you access the Drill Web Console must have an active Drillbit running.
2. Select Profiles in the toolbar.
A list of running and completed queries appears.
3. Click the query for which you want to see the profile.
4. Select **Edit Query**.
5. Click **Cancel** query to cancel the query.  

The following message appears:  

       Cancelled query <QueryID\>





