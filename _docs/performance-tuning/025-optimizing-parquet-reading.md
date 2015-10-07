---
title: "Optimizing Parquet Reading"
parent: "Performance Tuning"
---

Parquet metadata caching is an optional feature in Drill 1.2 and later. When you use this feature, Drill generates a metadata cache file. Drill stores the metadata cache file  in a directory you specify and its subdirectories. When you run a query on this directory or a subdirectory, Drill reads a single metadata cache file instead of retrieving metadata from multiple Parquet files during the query-planning phase.

## When to Use Parquet Metadata Caching

The scenarios in which metadata caching is useful is when the planning time is a significant percentage of the total elapsed time of the query. If the query execution time is the dominant factor then metadata caching will have very little impact. Before using this feature, run an EXPLAIN plan on your query and compare its time to the total time of query execution. Use these timings to determine whether metadata caching will be useful.


## How to Trigger Generation of the Parquet Metadata Cache File

The following command generates the Parquet metadata cache file in the `<path to table>` and its subdirectories.

`REFRESH TABLE METADATA <path to table>`

You need to run this command on a directory, nested or flat, only once during the session. Only the first query gathers the metadata unless the Parquet data changes, for example, you delete some data. If you did not make changes to the Parquet data, subsequent queries encounter the up-to-date Parquet metadata files. There is no need for Drill to regenerate the metadata. If there are changes, the metadata needs updating, so Drill regenerates the Parquet metadata when you issue the next query.

The elapsed time of the first query that triggers regeneration of metadata can be greater than that of subsequent queries that use that metadata.

## Example of Generating Parquet Metadata

```
0: jdbc:drill:schema=dfs> REFRESH TABLE METADATA t1;
+-------+----------------------------------------------+
|  ok   |                   summary                    |
+-------+----------------------------------------------+
| true  | Successfully updated metadata for table t1.  |
+-------+----------------------------------------------+
1 row selected (0.445 seconds)
```

## How Drill Generates and Uses Parquet Metadata

After running the REFRESH TABLE METADATA command, Drill traverses directories in the case of nested directories to find the Parquet files. From the footers of the files, Drill gathers metadata, such as row counts and node affinity based on HDFS block locations. For each directory level, Drill saves a summary of the information from the footers in a single Parquet metadata cache file. The summary at each level covers that particular level and all child levels; consequently, after generating metadata, you can query nested directories from any level. In order to leverage the metadata cache file for a table t, the file must exist at the root directory of that table. For example, you can query a subdirectory of Parquet files because Drill stores a Parquet metadata cache file at each level.

At planning time, Drill reads only the metadata file. Parquet metadata caching has no effect on execution time. At execution time, Drill reads the actual files.