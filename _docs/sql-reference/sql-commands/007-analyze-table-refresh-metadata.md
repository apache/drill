---
title: "ANALYZE TABLE REFRESH METADATA"
parent: "SQL Commands"
date: 2020-01-30
---

Starting from Drill 1.17, you can store table metadata (including schema and computed statistics) into Drill Metastore.
This metadata will be used when querying a table for more optimal plan creation.

The Metastore is an Alpha feature; it is subject to change. We encourage you to try it and provide feedback.
Because the Metastore is in Beta, the SQL commands and Metastore formats may change in the next release.
{% include startnote.html %}In Drill 1.17, this feature is supported for Parquet tables only and is disabled by default.{% include endnote.html %}

To use the Drill Metastore, you must enable it at the session or system level with one of the following commands:

	SET `metastore.enabled` = true;
	ALTER SYSTEM SET `metastore.enabled` = true;

Alternatively, you can enable the option in the Drill Web UI at `http://<drill-hostname-or-ip-address>:8047/options`.

Once you enable the Metastore, the next step is to populate it with data. Drill can query a table whether that table
 has a Metastore entry or not. (If you are familiar with Hive, then you know that Hive requires that all tables have
 Hive Metastore entries before you can query them.) In Drill, only add data to the Metastore when doing so improves
 query performance. In general, large tables benefit from statistics more than small tables do.

Unlike Hive, Drill does not require you to declare a schema. Instead, Drill infers the schema by scanning your table 
 and computes some metadata like MIN / MAX column values and NULLS COUNT designated as "metadata" to be able to
 produce more optimizations like filter push-down, etc. If `planner.statistics.use` option is enabled, this command
 will also calculate and store table statistics into Drill Metastore.

Unlike Hive, Drill does not require you to declare a schema. Instead, Drill infers the schema by scanning your table
 in the same way as it is done during regular select.

## Syntax

The ANALYZE TABLE REFRESH METADATA statement supports the following syntax:

	ANALYZE TABLE [table_name] [COLUMNS {(col1, col2, ...) | NONE}]
	REFRESH METADATA ['level' LEVEL]
	[{COMPUTE | ESTIMATE} | STATISTICS
	[ SAMPLE number PERCENT ]]

## Parameters

*table_name*
The name of the table or directory for which Drill will collect table metadata. If the table does not exist, the table
 is temporary or if you do not have permission to read the table, the command fails and metadata is not collected and stored.

*COLUMNS (col1, col2, ...)*
Optional names of the column(s) for which Drill will compute and store statistics. The stored schema will include all
 table columns.

*COLUMNS NONE*
Drill will infer schema for all columns, but gather statistics for none of the columns.

*level*
Optional VARCHAR literal which specifies maximum level depth for collecting metadata.
Possible values:

- `TABLE` - metadata will be collected at table level (MIN / MAX column values within whole the table, etc.);
- `SEGMENT` - metadata will be collected for every segment within the table (MIN / MAX column values within a specific
 segment) + metadata at table level. Segment term here is used to abstract data part which may correspond to a
 specific directory on the file system or partition in Hive table;
- `PARTITION` - metadata will be collected for every partition within the table (MIN / MAX column values within a
 specific partition) + file, segment and table metadata. Partition term is used here to abstract part of the data
 where some column(s) have the same values (corresponds to existing Drill partitions for Parquet table). Is not
 implemented in Drill 1.17;
- `FILE` - metadata will be collected for every file within the table (MIN / MAX column values within a specific file) + 
 partition, segment metadata and table metadata;
- `ROW_GROUP` - metadata will be collected for every row group within the table (MIN / MAX column values within a
 specific row group) + file, partition, segment metadata and table metadata. Supported for Parquet tables only;
- `ALL` - metadata will be collected for every splittable table part - row groups for parquet, files for regular file
 storage formats, etc.
Default is `ALL`.

*COMPUTE*
Computes statistics for the table to be stored into the Metastore.
If statistics usage is disabled (`planner.statistics.use` is set to `false`), an error will be thrown when this clause is specified.

*ESTIMATE*
Computes estimated statistics for the table to be stored into the Metastore. Currently is not supported.

*(column1, column2, ...)*
The name of the column(s) for which Drill will compute statistics.

*SAMPLE*
Optional. Indicates that compute statistics should run on a subset of the data.

*number PERCENT*  
An integer that specifies the percentage of data on which to compute statistics. For example, if a table has 100 rows,
 `SAMPLE 50 PERCENT` indicates that statistics should be computed on 50 rows. The optimizer selects the rows at random. 

## Related Commands

Use the following command to remove a table from the Metastore:

	ANALYZE TABLE [table_name] DROP [METADATA|STATISTICS] [IF EXISTS]

The command will fail if the table does not exist in the Metastore. Include the `IF EXISTS` clause to ignore a missing table.

	ANALYZE TABLE [plugin.schema.]table_name COMPUTE STATISTICS [(column1, column2,...)] [SAMPLE number PERCENT]

See [ANALYZE TABLE COMPUTE STATISTICS]({{site.baseurl}}/docs/analyze-table-compute-statistics).

	REFRESH TABLE METADATA  [ COLUMNS ( column1, column2...) | NONE ]  table_path

For the case when table metadata was stored into the Drill Metastore, Parquet table metadata cache files, wouldn't be
 used for the same table during query execution if all required metadata is present and is not outdated.

## Usage Notes

For more detailed information on Drill Metastore and its usage please refer to [Using Drill Metastore]({{site.baseurl}}/docs/using-drill-metastore).
