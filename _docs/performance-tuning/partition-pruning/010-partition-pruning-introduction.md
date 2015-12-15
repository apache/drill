---
title: "Partition Pruning Introduction"
parent: "Partition Pruning"
--- 

Partition pruning is a performance optimization that limits the number of files and partitions that Drill reads when querying file systems and Hive tables. When you partition data, Drill only reads a subset of the files that reside in a file system or a subset of the partitions in a Hive table when a query matches certain filter criteria.

The query planner in Drill performs partition pruning by evaluating the filters. If no partition filters are present, the underlying Scan operator reads all files in all directories and then sends the data to operators, such as Filter, downstream. When partition filters are present, the query planner pushes the filters down to the Scan if possible. The Scan reads only the directories that match the partition filters, thus reducing disk I/O.

## Using Partitioned Drill Data
Before using Parquet data created by Drill 1.2 or earlier in later releases, you need to migrate the data. Migrate Parquet data as described in ["Migrating Parquet Data"]({{site.baseurl}}/docs/migrating-parquet-data/). 

{% include startimportant.html %}Migrate only Parquet files that Drill generated.{% include endimportant.html %}

## Partitioning Data
In early versions of Drill, partition pruning involved time-consuming manual setup tasks. Using the PARTITION BY clause in the CTAS command simplifies the process.





