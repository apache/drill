---
title: "Optimizing Parquet Reading"
parent: "Performance Tuning"
---

Parquet metadata caching is an optional feature in Drill 1.2 and later. When you use this feature, Drill generates a metadata cache file. Drill stores the metadata cache file  in a directory you specify and its subdirectories. When you run a query on this directory or a subdirectory, Drill reads the metadata cache file instead of the actual files that contain the metadata during the query-planning phase. You can realize performance improvements during the query-planning phase when Drill reads just one metadata file instead of reading multiple files to fetch the metadata.

The Parquet metadata caching feature enables Drill to speed up the cost of query planning. The actual query runtime will not be improved if the planning cost is not a bottleneck.

## When to Use Parquet Metadata Caching

Use Parquet metadata caching to optimize reads only when the planning phase of a query takes longer than the execution phase. Before using this feature, run your query and compare the time for executing the logical (planning) and physical (execution) operations to see if using this feature makes sense. The logical/planning operations must take longer than the physical/execution operations; otherwise, do not create Parquet metadata because Drill cannot optimize reads under these conditions.

## How to Trigger Generation of the Parquet Metadata Cache File

The following command generates the Parquet metadata cache file in the `<path to table>` and its subdirectories.

`REFRESH TABLE METADATA <path to table>`

You need to run this command on a directory, nested or flat, only once during the session. Only the first query gathers the metadata unless the Parquet data changes, for example, you delete some data. If you did not make changes to the Parquet data, subsequent queries encounter the up-to-date Parquet metadata files. There is no need for Drill to regenerate the metadata. If there are changes, the metadata needs updating, so Drill regenerates the Parquet metadata when you issue the next query.

The elapsed time of any queries that trigger regeneration of data can be greater than that of other queries.

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

After running the REFRESH TABLE METADATA command, Drill traverses directories in the case of nested directories to find the Parquet files. From the footers of the files, Drill gathers metadata, such as row counts and node affinity based on HDFS block locations. For each directory level, Drill saves a summary of the information from the footers in a single Parquet metadata cache file. The summary at each level covers that particular level and all lower levels; consequently, after generating metadata, you can query nested directories from any level. For example, you can query a subdirectory of Parquet files because Drill stores a Parquet metadata cache file at each level.

At planning time, Drill reads only the metadata file. Parquet metadata caching has no effect on execution time. At execution time, Drill reads the actual files.