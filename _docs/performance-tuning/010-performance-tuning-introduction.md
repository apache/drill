---
title: "Performance Tuning Introduction"
date:  
parent: "Performance Tuning"
---
You can change system options in Drill to improve the query performance. Before you improve performance in Drill, you must choose a layout of the data and the choose an appropriate file format specific to your use case. For example, for an analytic workload operating on historical time series data, then choosing Parquet as the file format and a partitioning scheme that uses time as a partitionining dimension would be a recommended approach. In the case you are directly querying data data sources, you need to have an understanding of the data source itself. Some familiarity with how Drill executes queries can also help. 

You can analyze query plans and profiles to identify performance bottlenecks in Drill. Once you identified issue, here are a couple of best practices to get you started:

* Modify query planning options
* Modify broadcast join options
* Switch between 1 or 2 phase aggregation
* Enable/disable hash-based memory-constrained operators
* Enable query queuing
* Control parallelization
* Organize data for partition pruning
* Change storage formats
* Disable Logging (See Logging and Debugging)
