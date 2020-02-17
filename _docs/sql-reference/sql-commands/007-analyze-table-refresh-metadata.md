---
title: "ANALYZE TABLE REFRESH METADATA"
parent: "SQL Commands"
date: 2020-03-03
---

Starting from Drill 1.17, you can store table metadata (including schema and computed statistics) into Drill Metastore.
This metadata will be used when querying a table for more optimal plan creation.

To use the Drill Metastore, you must enable it at the session or system level with one of the following commands:

	SET `metastore.enabled` = true;
	ALTER SYSTEM SET `metastore.enabled` = true;

Please refer to [Using Drill Metastore]({{site.baseurl}}/docs/using-drill-metastore) for more details about Drill Metastore including its purpose and how to use it.

## Syntax

The ANALYZE TABLE REFRESH METADATA statement supports the following syntax:

	ANALYZE TABLE [table_name | table({table function name}(parameters))]
	[COLUMNS {(col1, col2, ...) | NONE}]
	REFRESH METADATA ['level' LEVEL]
	[{COMPUTE | ESTIMATE} | STATISTICS
	[ SAMPLE number PERCENT ]]

## Parameters

*table_name*
The name of the table or directory for which Drill will collect table metadata. If the table does not exist, the table
 is temporary or if you do not have permission to read the table, the command fails and metadata is not collected and stored.

*table({table function name}(parameters))*
Table function parameters. This syntax is only available since Drill 1.18.
Example of table function parameters usage:

    table(dfs.`table_name` (type => 'parquet', autoCorrectCorruptDates => true))

For detailed information, please refer to
 [Using the Formats Attributes as Table Function Parameters]({{site.baseurl}}/docs/plugin-configuration-basics/#using-the-formats-attributes-as-table-function-parameters)

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
