---
title: "Index Selection"
date: 2018-09-28 21:35:21 UTC
parent: "Querying Indexes"
---  

Drill leverages indexes during the physical planning phase of the query. The query planner in Drill evaluates the cost of multiple query plans and then selects the plan with the lowest cost for query execution. The planner uses CPU, memory, and network I/O estimates to calculate cost. Based on these costs, the query planner evaluates selectivity, collation (sortedness of data), and the type of index. For each candidate index, the query planner estimates the total cost of accessing the index.  

The query planner can create three types of index plans: covering, non-covering, and index intersection. By default, the planner evaluates the top five indexes per table, though this number is configurable. The indexes chosen may also include an index intersection. For example, two indexes may not qualify for an index plan based on their individual selectivity, but their combined selectivity after intersection could be low enough to qualify.  
 
##Selectivity    
Selectivity is the estimated number of rows based on the selectivity of each conditional expression in the WHERE clause, calculated as (output row count)/(total table row count). For example, if a table has 100 rows and 25 of the rows qualify the filter condition, the selectivity is .25. Selectivity ranges between 0 and 1. The closer to 0, the more selective the filter. The more selective a filter, the lower the cost. High filter selectivity results in better query performance. If filter conditions are not selective enough for the planner to choose the index, consider removing the index to free up storage.  

The data source provides Drill with the estimated number of rows that match a filter filter condition and the average row size. The data source uses a filter condition, such as WHERE a > 10 AND b < 20, to return the estimated row count of the filter condition based on the leading prefix of the index columns. For example, if the index columns are the composite key {a, b}, the leading prefix of the filter condition is {a, b}, and the row count of the conjunct (number of rows that meet both filter conditions) is used. However, if the index columns are {b, c}, the only filter condition used to estimate the row count is b < 20. The planner uses the filter condition and the index metadata to determine the leading prefix.  

##Selectivity of Covering and Non-Covering Indexes
For a covering index, the query planner always generates a covering index plan, even if the estimated selectivity is 100%. The planner always expects an index-only plan to be cheaper than a plan with a full table scan due to the smaller row widths in an index.  
  
For a non-covering index, the query planner estimates the cost of a join-back to the primary table. Due to the random I/O nature of the rowkey join-back to primary table, the default selectivity threshold is small: 2.5% (.025). You can configure the default selectivity threshold for non-covering indexes through the `planner.index.noncovering_selectivity_threshold` option.   

If the estimated selectivity of the filter condition is above this threshold, the query planner does not generate a non-covering index plan for that index; the rationale is that each new plan adds to the search space and increases planning time. If the estimated row count is already high, the plan is unlikely to be chosen anyway, and therefore better to prune the plan out early. 

 