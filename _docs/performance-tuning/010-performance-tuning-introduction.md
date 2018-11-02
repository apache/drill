---
title: "Performance Tuning Introduction"
date: 2018-11-02
parent: "Performance Tuning"
---
You can apply performance tuning measures to improve how efficiently Drill queries data. To significantly improve performance in Drill, you must have knowledge about the underlying data and data sources, as well as familiarity with how Drill executes queries.

You can analyze query plans and profiles to identify the source of performance issues in Drill. Once you have isolated the source of an issue, you can apply the following tuning techniques to improve query performance:

* Modify query planning options
* Modify broadcast join options
* Switch between 1 or 2 phase aggregation
* Enable/disable hash-based memory-constrained operators
* Enable query queuing
* Control parallelization
* Organize data for partition pruning
* Change storage formats
* Disable Logging (See Logging and Debugging)
