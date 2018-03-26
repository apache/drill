---
title: "Partition Pruning Introduction"
date: 2018-03-26 17:37:51 UTC
parent: "Partition Pruning"
--- 

Partition pruning is a performance optimization that limits the number of files and partitions that Drill reads when querying file systems and Hive tables. When you partition data, Drill only reads a subset of the files that reside in a file system or a subset of the partitions in a Hive table when a query matches certain filter criteria.

As of Drill 1.8, partition pruning also applies to the Parquet metadata cache. When data is partitioned in a directory hierarchy, Drill attempts to read the metadata cache file from a sub-partition, based on matching filter criteria instead of reading from the top level partition, to reduce the amount of metadata read during the query planning time. If you created a metadata cache file in a previous version of Drill, you must issue the REFRESH TABLE METADATA command to regenerate the metadata cache file before running queries for metadata cache pruning to occur. See [Optimizing Parquet Metadata Reading]({{site.baseurl}}/docs/optimizing-parquet-metadata-reading/) for more information.  

The query planner in Drill performs partition pruning by evaluating the filters. If no partition filters are present, the underlying Scan operator reads all files in all directories and then sends the data to operators, such as Filter, downstream. When partition filters are present, the query planner pushes the filters down to the Scan if possible. The Scan reads only the directories that match the partition filters, thus reducing disk I/O.  

As of Drill 1.13, the query planner in Drill can apply project push down, filter push down, and partition pruning to star queries in common table expressions (CTEs), views, and subqueries, for example:  
  
       select col1 from (select * from t)  

When a CTE, view, or subquery contains a star filter condition, the query planner in Drill can apply the filter and prune extraneous data, further reducing the amount of data that the scanner reads and improving performance. 
 
**Note:** Currently, Drill only supports pushdown for simple star subselect queries without filters. See [DRILL-6219](https://www.google.com/url?q=https://issues.apache.org/jira/browse/DRILL-6219&sa=D&ust=1522084453671000&usg=AFQjCNFXp-nWMRXzM466BSRFlV3F63_ZYA) for more information.

## Using Partitioned Drill Data
Before using Parquet data created by Drill 1.2 or earlier in later releases, you need to migrate the data. Migrate Parquet data as described in ["Migrating Parquet Data"]({{site.baseurl}}/docs/migrating-parquet-data/). 

{% include startimportant.html %}Migrate only Parquet files that Drill generated.{% include endimportant.html %}

## Partitioning Data
In early versions of Drill, partition pruning involved time-consuming manual setup tasks. Using the PARTITION BY clause in the CTAS command simplifies the process.





