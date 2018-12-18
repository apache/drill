---
title: "Querying Indexes Introduction"
date: 2018-12-18
parent: "Querying Indexes"
---  

{% include startnote.html %}Currently, Drill only supports indexes for the MapR-DB storage plugin. Examples in this document were created using Drill on MapR Database. {% include endnote.html %}

An index is a structure, defined on a table, that contains a subset of columns from the table sorted by key values. Well-designed indexes eliminate full table scans and optimize access to data to significantly improve performance. Starting in Drill 1.15, Drill can leverage indexes (primary or secondary) in data sources to create index-based query plans. 

{% include startnote.html %}You cannot create, update, or delete indexes or tables through Drill. You must create and manage the indexes and tables in the data sources themselves. Also, the data source must be configured as a storage plugin in Drill.{% include endnote.html %}

An index-based query plan is a query plan that uses indexes versus full tables scans to access data. When you submit a query through Drill, the query planner in Drill evaluates the query and compares the cost of multiple query plans to find an optimal plan with the lowest cost. Index planning is enabled by default; however, you can disable the feature or configure the behavior of index planning in Drill through several options. 

Certain types of queries qualify for index-based query plans. You can create indexes and write queries such that the query planner in Drill leverages the indexes to create an index-based query plan for optimized performance. Drill can leverage indexes to create covering, non-covering, and functional index plans. You can verify that the query planner generated an index-based query plan in the query profile (Drill Web UI) or from the command line using the [EXPLAIN PLAN FOR]({{site.baseurl}}/docs/explain/#explain-for-physical-plans) command. 
