---
title: "Partition Pruning Introduction"
date: 2016-08-15 18:40:27 UTC
parent: "Partition Pruning"
--- 

Partition pruning is a performance optimization that limits the number of files and partitions that Drill reads when querying file systems and Hive tables. When you partition data, Drill only reads a subset of the files that reside in a file system or a subset of the partitions in a Hive table when a query matches certain filter criteria.

As of Drill 1.8, partition pruning also applies to the Parquet metadata cache. When data is partitioned in a directory hierarchy, Drill attempts to read the metadata cache file from a sub-partition, based on matching filter criteria instead of reading from the top level partition, to reduce the amount of metadata read during the query planning time. If you created a metadata cache file in a previous version of Drill, you must issue the REFRESH TABLE METADATA command to regenerate the metadata cache file before running queries for metadata cache pruning to occur. See [Optimizing Parquet Metadata Reading]({{site.baseurl}}/docs/optimizing-parquet-metadata-reading/) for more information.  

The query planner in Drill performs partition pruning by evaluating the filters. If no partition filters are present, the underlying Scan operator reads all files in all directories and then sends the data to operators, such as Filter, downstream. When partition filters are present, the query planner pushes the filters down to the Scan if possible. The Scan reads only the directories that match the partition filters, thus reducing disk I/O.

## Using Partitioned Drill Data
Before using Parquet data created by Drill 1.2 or earlier in later releases, you need to migrate the data. Migrate Parquet data as described in ["Migrating Parquet Data"]({{site.baseurl}}/docs/migrating-parquet-data/). 

{% include startimportant.html %}Migrate only Parquet files that Drill generated.{% include endimportant.html %}

## Partitioning Data
In early versions of Drill, partition pruning involved time-consuming manual setup tasks. Using the PARTITION BY clause in the CTAS command simplifies the process.





