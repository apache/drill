---
title: "Guidelines for Optimizing Aggregation"
date: 2016-11-21 22:28:43 UTC
parent: "Query Plans and Tuning"
--- 


For queries that contain GROUP BY, Drill performs aggregations in either 1 or 2 phases.  In both of these schemes, Drill can use the Hash Aggregate and Streaming Aggregate physical operators.  The default behavior in Drill is to perform 2 phase aggregation.  
 
In the 2 phase aggregation scheme, each minor fragment performs local (partial) aggregation in phase 1.  It then sends the partially aggregated results to other fragments using a hash-based distribution operator.  The hash distribution is done on the GROUP BY keys.  In phase 2 all of the fragments perform a total aggregation using data received from phase 1.  
 
The 2 phase aggregation scheme is very efficient when the data contains grouping keys with a reasonable number of duplicate values such that doing the grouping reduces the number of rows sent to downstream operators.  However, if there is not much reduction it is best to use 1 phase aggregation.   
 
For example, suppose the query does a GROUP BY x, y.  If the combination of {x, y} values is unique (or nearly unique) in all of the rows of the input data, then there is no reduction in the number of rows when performing the grouping.  In this case, performance improves by doing 1 phase aggregation.  
 
You can use the ALTER SYSTEM or ALTER SESSION commands with the following option to control aggregation in Drill:

*  planner.enable\_multiphase\_agg 

 
The default for this option is `true`.Typically, you set the options at the session level unless you want the setting to persist across all sessions.
 
