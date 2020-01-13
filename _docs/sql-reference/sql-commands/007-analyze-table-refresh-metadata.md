---
title: "ANALYZE TABLE REFRESH METADATA"
parent: "SQL Commands"
date: 2020-01-13
---

Starting from Drill 1.17, you can store table metadata (including schema and computed statistics) into Drill Metastore.
This metadata will be used when querying a table for more optimal plan creation.

{% include startnote.html %}In Drill 1.17, this feature is supported for Parquet tables only and is disabled by default.{% include endnote.html %}

To enable Drill Metastore usage, the following option `metastore.enabled` should be set to `true`, as shown:

	SET `metastore.enabled` = true;

Alternatively, you can enable the option in the Drill Web UI at `http://<drill-hostname-or-ip-address>:8047/options`.

## Syntax

The ANALYZE TABLE REFRESH METADATA statement supports the following syntax:

	ANALYZE TABLE [table_name] [COLUMNS {(col1, col2, ...) | NONE}]
	REFRESH METADATA ['level' LEVEL]
	[{COMPUTE | ESTIMATE} | STATISTICS [(column1, column2, ...)]
	[ SAMPLE number PERCENT ]]

## Parameters

*table_name*
The name of the table or directory for which Drill will collect table metadata. If the table does not exist, or the table
 is temporary, the command fails and metadata is not collected and stored.

*COLUMNS (col1, col2, ...)*
Optional names of the column(s) for which Drill will generate and store metadata. the Stored schema will include all table columns.

*COLUMNS NONE*
Specifies to ignore collecting and storing metadata for all table columns.

*level*
Optional varchar literal which specifies maximum level depth for collecting metadata.
Possible values: `TABLE`, `SEGMENT`, `PARTITION`, `FILE`, `ROW_GROUP`, `ALL`. Default is `ALL`.

*COMPUTE*
Generates statistics for the table to be stored into the Metastore.
If statistics usage is disabled (`planner.enable_statistics` is set to `false`), an error will be thrown when this clause is specified.

*ESTIMATE*
Generates estimated statistics for the table to be stored into the Metastore. Currently is not supported.

*(column1, column2, ...)*
The name of the column(s) for which Drill will generate statistics.

*SAMPLE*
Optional. Indicates that compute statistics should run on a subset of the data.

*number PERCENT*  
An integer that specifies the percentage of data on which to compute statistics. For example, if a table has 100 rows, `SAMPLE 50 PERCENT` indicates that statistics should be computed on 50 rows. The optimizer selects the rows at random. 

## Related Options

- **metastore.enabled**
Enables Drill Metastore usage to be able to store table metadata during ANALYZE TABLE commands execution and to be able
 to read table metadata during regular queries execution or when querying some INFORMATION_SCHEMA tables. Default is `false`.
- **metastore.metadata.store.depth_level**
Specifies maximum level depth for collecting metadata. Default is `'ALL'`.
- **metastore.retrieval.retry_attempts**
Specifies the number of attempts for retrying query planning after detecting that query metadata is changed.
If the number of retries was exceeded, query will be planned without metadata information from the Metastore. Default is 5.
- **metastore.metadata.fallback_to_file_metadata**
Allows using file metadata cache for the case when required metadata is absent in the Metastore. Default is true.
- **metastore.metadata.use_schema**
Enables schema usage, stored to the Metastore. Default is `true`.
- **metastore.metadata.use_statistics**
Enables statistics usage, stored in the Metastore, at the planning stage. Default is `true`.
- **metastore.metadata.ctas.auto-collect**
Specifies whether schema and / or column statistics will be automatically collected for every table after CTAS and CTTAS.
This option is not active for now. Default is `'NONE'`.
- **drill.exec.storage.implicit.last_modified_time.column.label**
Sets the implicit column name for the last modified time (`lmt`) column. For internal usage when producing Metastore analyze.
- **drill.exec.storage.implicit.row_group_index.column.label**
Sets the implicit column name for the row group index (`rgi`) column. For internal usage when producing Metastore analyze.
- **drill.exec.storage.implicit.row_group_length.column.label**
Sets the implicit column name for the row group length (`rgl`) column. For internal usage when producing Metastore analyze.
- **drill.exec.storage.implicit.row_group_start.column.label**
Sets the implicit column name for the row group start (`rgs`) column. For internal usage when producing Metastore analyze.

## Related Commands

To drop table metadata from the Metastore, the following command may be used:

	ANALYZE TABLE [table_name] DROP [METADATA|STATISTICS] [IF EXISTS]

It will not throw an exception for absent table metadata if `IF EXISTS` clause was specified.

	ANALYZE TABLE [workspace.]table_name COMPUTE STATISTICS [(column1, column2,...)] [SAMPLE number PERCENT]
	
See [ANALYZE TABLE COMPUTE STATISTICS]({{site.baseurl}}/docs/analyze-table-compute-statistics). 

## Usage Notes

### General Information

- Currently `ANALYZE TABLE REFRESH METADATA` statement can compute and store metadata only for Parquet tables within `dfs` storage plugins.
- For the case when `ANALYZE TABLE REFRESH METADATA` command is executed for the first time, whole table metadata will be collected and stored into Metastore.
If analyze was already executed for the table, and table data wasn't changed, all further analyze commands wouldn't trigger table analyzing and message that table metadata is up to date will be returned.

### Incremental analyze

For the case when some table data was updated, Drill will try to execute incremental analyze - calculate metadata only for updated data and reuse required metadata from the Metastore.

Incremental analyze wouldn't be produced for the following cases:
- list of interesting columns specified in analyze is not a subset of interesting columns from the previous analyze;
- specified metadata level differs from the metadata level in previous analyze.

### Metadata usage

Drill provides the ability to use metadata obtained from the Metastore at the planning stage to prune segments, files
 and row groups.

Tables metadata from the Metastore is exposed to `INFORMATION_SCHEMA` tables (if Metastore usage is enabled).

The following tables are populated with table metadata from the Metastore:

`TABLES` table has the following additional columns populated from the Metastore:

- `TABLE_SOURCE` - table data type: `PARQUET`, `CSV`, `JSON`
- `LOCATION` - table location: `/tmp/nation`
- `NUM_ROWS` - number of rows in a table if known, `null` if not known
- `LAST_MODIFIED_TIME` - table's last modification time

`COLUMNS` table has the following additional columns populated from the Metastore:

- `COLUMN_DEFAULT` - column default value
- `COLUMN_FORMAT` - usually applicable for date time columns: `yyyy-MM-dd`
- `NUM_NULLS` - number of nulls in column values
- `MIN_VAL` - column min value in String representation: `aaa`
- `MAX_VAL` - column max value in String representation: `zzz`
- `NDV` - number of distinct values in column, expressed in Double
- `EST_NUM_NON_NULLS` - estimated number of non null values, expressed in Double
- `IS_NESTED` - if column is nested. Nested columns are extracted from columns with struct type.

`PARTITIONS` table has the following additional columns populated from the Metastore:

- `TABLE_CATALOG` - table catalog (currently we have only one catalog): `DRILL`
- `TABLE_SCHEMA` - table schema: `dfs.tmp`
- `TABLE_NAME` - table name: `nation`
- `METADATA_KEY` - top level segment key, the same for all nested segments and partitions: `part_int=3`
- `METADATA_TYPE` - `SEGMENT` or `PARTITION`
- `METADATA_IDENTIFIER` - current metadata identifier: `part_int=3/part_varchar=g`
- `PARTITION_COLUMN` - partition column name: `part_varchar`
- `PARTITION_VALUE` - partition column value: `g`
- `LOCATION` - segment location, `null` for partitions: `/tmp/nation/part_int=3`
- `LAST_MODIFIED_TIME` - last modification time

## Limitations

This feature is currently in the alpha phase (preview, experimental) for Drill 1.17 and only applies to Parquet
 tables in this release. You must enable this feature through the `metastore.enabled` system/session option.
