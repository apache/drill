---
title: "Optimizing Parquet Metadata Reading"
date: 2017-08-10 22:24:37 UTC
parent: "Performance Tuning"
---

Parquet metadata caching is a feature that enables Drill to read a single metadata cache file instead of retrieving metadata from multiple Parquet files during the query-planning phase. 
Parquet metadata caching is available for Parquet data in Drill 1.2 and later. To enable Parquet metadata caching, issue the REFRESH TABLE METADATA <path to table> command. When you run this command Drill generates a metadata cache file.  

{% include startnote.html %}Parquet metadata caching does not benefit queries on Hive tables, HBase tables, or text files.{% include endnote.html %}  

Drill stores the metadata cache file in the specified directory and subdirectories. When you run a query on this directory or subdirectories, Drill reads the metadata cache file instead of retrieving metadata from multiple Parquet files during the query-planning phase.     

In Drill 1.11 and later, Drill stores the paths to the Parquet files as relative paths instead of absolute paths. You can move partitioned Parquet directories from one location in the distributed files system to another without issuing the REFRESH TABLE METADATA command to rebuild the Parquet metadata files; the metadata remains valid in the new location.   

{% include startnote.html %}Reverting back to a previous version of Drill from 1.11 is not recommended because Drill will incorrectly interpret the Parquet metadata files created by Drill 1.11. Should this occur, remove the Parquet metadata files and run the refresh table metadata command to rebuild the files in the older format.{% include endnote.html %} 
 

## When to Use Parquet Metadata Caching

Metadata caching is useful when planning time is a significant percentage of the total elapsed time of the query. If the query execution time is the dominant factor, which is typically observed with a large number of files, then metadata caching will have very little impact. To determine that query execution time is the dominant factor, run an EXPLAIN plan on your query of a large number of files, and compare its time to the total time of query execution. Use the comparison to determine whether metadata caching will be useful.

When enabled, Drill always uses the Parquet metadata cache during the query-planning phase. To optimize reading Parquet metadata, make sure the metadata cache is up-to-date after making any changes, such as inserts, to the data in the cluster. The next section describes how to update the metadata cache.


## Generating the Parquet Metadata Cache File

The following command generates the Parquet metadata cache file in the `<path to table>` and its subdirectories.

       REFRESH TABLE METADATA <path to table>

You need to run this command on a directory, nested or flat, only once during the session. Only the first query gathers the metadata unless the Parquet data changes, for example, you delete some data. If you did not make changes to the Parquet data, subsequent queries encounter the up-to-date Parquet metadata files. There is no need for Drill to regenerate the metadata. If there are changes, the metadata needs updating, so Drill dynamically regenerates the Parquet metadata when you issue the next query.

The elapsed time of the first query that triggers regeneration of metadata can be greater than that of subsequent queries that use that metadata. If this increase in the time of the first query is unacceptable, make sure the cache is up-to-date by running the REFRESH TABLE METADATA command, as shown in the following example:


       0: jdbc:drill:schema=dfs> REFRESH TABLE METADATA t1;
       +-------+----------------------------------------------+
       |  ok   |                   summary                    |
       +-------+----------------------------------------------+
       | true  | Successfully updated metadata for table t1.  |
       +-------+----------------------------------------------+
       1 row selected (0.445 seconds)  
  


## How Drill Generates and Uses Parquet Metadata

After running the REFRESH TABLE METADATA command, Drill traverses directories in the case of nested directories to find the Parquet files. From the footers of the files, Drill gathers metadata, such as row counts and node affinity based on HDFS block locations. For each directory level, Drill saves a summary of the information from the footers in a single Parquet metadata cache file. The summary at each level covers that particular level and all child levels; consequently, after generating metadata, you can query nested directories from any level. In order to leverage the metadata cache file for a table t, the file must exist at the root directory of that table. For example, you can query a subdirectory of Parquet files because Drill stores a Parquet metadata cache file at each level.

At planning time, Drill reads only the metadata file. Parquet metadata caching has no effect on execution time. At execution time, Drill reads the actual files.  

<!--
## Security Limitations
TBD  

-->