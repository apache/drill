---
title: "Using Drill Metastore"
parent: "Drill Metastore"
date: 2020-01-31
---

Drill 1.17 introduces the Drill Metastore which stores the table schema and table statistics. Statistics allow Drill to better create optimal query plans.

The Metastore is a Beta feature; it is subject to change. We encourage you to try it and provide feedback.
Because the Metastore is in Beta, the SQL commands and Metastore formats may change in the next release.
{% include startnote.html %}In Drill 1.17, this feature is supported for Parquet tables only and is disabled by default.{% include endnote.html %}

## Enabling Drill Metastore

To use the Drill Metastore, you must enable it at the session or system level with one of the following commands:

	SET `metastore.enabled` = true;
	ALTER SYSTEM SET `metastore.enabled` = true;

Alternatively, you can enable the option in the Drill Web UI at `http://<drill-hostname-or-ip-address>:8047/options`.

## Computing and storing table metadata to Drill Metastore

Once you enable the Metastore, the next step is to populate it with data. Drill can query a table whether that table
 has a Metastore entry or not. (If you are familiar with Hive, then you know that Hive requires that all tables have
 Hive Metastore entries before you can query them.) In Drill, only add data to the Metastore when doing so improves
 query performance. In general, large tables benefit from statistics more than small tables do.

Unlike Hive, Drill does not require you to declare a schema. Instead, Drill infers the schema by scanning your table 
 in the same way as it is done during regular select and computes some metadata like `MIN` / `MAX` column values and
 `NULLS_COUNT` designated as "metadata" to be able to produce more optimizations like filter push-down, etc. If
 `planner.statistics.use` option is enabled, this command will also calculate and store table statistics into Drill
 Metastore.

## Configuration

Default Metastore configuration is defined in `drill-metastore-default.conf` file.
It can be overridden in `drill-metastore-override.conf`. Distribution configuration can be
indicated in `drill-metastore-distrib.conf`.

All configuration properties should reside in `drill.metastore` namespace.
Metastore implementation based on class implementation config property `drill.metastore.implementation.class`.
The default value is the following:

```
drill.metastore: {
  implementation.class: "org.apache.drill.metastore.iceberg.IcebergMetastore"
}
```

Note, that currently out of box Iceberg Metastore is available and is the default one. Though any custom
 implementation can be added by placing the JAR into classpath which has the implementation of
 `org.apache.drill.metastore.Metastore` interface and indicating custom class in the `drill.metastore.implementation.class`.

### Metastore Components

Metastore can store metadata for various components: tables, views, etc.
Current implementation provides fully functioning support for tables component.
Views component support is not implemented but contains stub methods to show
how new Metastore components like UDFs, storage plugins, etc. can be added in the future.

### Metastore Tables

Metastore Tables component contains metadata about Drill tables, including general information, as well as
information about table segments, files, row groups, partitions.

Full table metadata consists of two major concepts: general information and top-level segments metadata.
Table general information contains basic table information and corresponds to the `BaseTableMetadata` class.

A table can be non-partitioned and partitioned. Non-partitioned tables have only one top-level segment 
which is called default (`MetadataInfo#DEFAULT_SEGMENT_KEY`). Partitioned tables may have several top-level segments.
Each top-level segment can include metadata about inner segments, files, row groups, and partitions.

A unique table identifier in Metastore Tables is a combination of storage plugin, workspace, and table name.
Table metadata inside is grouped by top-level segments, unique identifier of the top-level segment and its metadata
is storage plugin, workspace, table name, and metadata key.

### Related Session/System Options

The following options are set via `ALTER SYSTEM SET`, or `ALTER SESSION SET` or via the Drill Web console.

- **metastore.enabled**
Enables Drill Metastore usage to be able to store table metadata during ANALYZE TABLE commands execution and to be able
 to read table metadata during regular queries execution or when querying some INFORMATION_SCHEMA tables. Default is `false`.
- **metastore.metadata.store.depth_level**
Specifies the maximum level of depth for collecting metadata. Same options as the _level_ option above. Default is `'ALL'`.
- **metastore.retrieval.retry_attempts**
If you run the `ANALYZE TABLE` command at the same time as queries run, then the query can read incorrect or corrupt statistics.
Drill will reload statistics and replan the query. This option specifies the maximum number of retry attempts. Default is `5`.
- **metastore.metadata.fallback_to_file_metadata**
Allows using [file metadata cache]({{site.baseurl}}/docs/refresh-table-metadata) for the case when required metadata is absent in the Metastore.
Default is `true`.
- **metastore.metadata.use_schema**
The `ANALYZE TABLE` command infers table schema as it gathers statistics. This option tells Drill to use that schema information while planning the query. 
Disable this option if Drill has inferred the schema incorrectly, or schema will be provided separately (see [CREATE OR REPLACE SCHEMA]({{site.baseurl}}/docs/create-or-replace-schema)).
Default is `true`.
- **metastore.metadata.use_statistics**
Enables obtaining table and column statistics, stored in the Metastore, at the planning stage. Default is `true`.
Enable `planner.statistics.use` to be able to use statistics during query planning.
- **metastore.metadata.ctas.auto-collect**
Drill provides the [`CREATE TABLE AS`]({{site.baseurl}}/docs/create-or-replace-schema) commands to create new tables.
This option causes Drill to gather schema and statistics for those tables automatically as they are written.
This option is not implemented for now. Possible values: `'ALL'`, `'SCHEMA'`, `'NONE'`. Default is `'NONE'`.
- **drill.exec.storage.implicit.last_modified_time.column.label**
Sets the implicit column name for the last modified time (`lmt`) column. Used when producing Metastore analyze. You can
 set the last modified time column name to custom name when current column name clashes which column name present in the
 table. If your table contains a column name with the same name as an implicit column, the implicit column takes
 priority and shadows column from the table.
- **drill.exec.storage.implicit.row_group_index.column.label**
Sets the implicit column name for the row group index (`rgi`) column. Used when producing Metastore analyze. You can
 set row group index column name to custom name when current column name clashes which column name present in the
 table. If your table contains a column name with the same name as an implicit column, the implicit column takes
 priority and shadows column from the table.
- **drill.exec.storage.implicit.row_group_length.column.label**
Sets the implicit column name for the row group length (`rgl`) column. Used when producing Metastore analyze. You can
 set row group length column name to custom name when current column name clashes which column name present in the
 table. If your table contains a column name with the same name as an implicit column, the implicit column takes
 priority and shadows column from the table.
- **drill.exec.storage.implicit.row_group_start.column.label**
Sets the implicit column name for the row group start (`rgs`) column. Used when producing Metastore analyze. You can
 set row group start column name to custom name when current column name clashes which column name present in the
 table. If your table contains a column name with the same name as an implicit column, the implicit column takes
 priority and shadows column from the table.

## Incremental analysis

If you have computed statistics for a table, and issue `ANALYZE TABLE` a second time, Drill will attempt to update
 statistics, called "incremental analysis."
Incremental analysis will compute metadata only for files and partitions changed since the last analysis and reuse
 actual metadata from the Metastore where possible.

Drill performs incremental analysis only when the `ANALYZE TABLE command` is identical to the previous command:
- The list of columns in the `COLUMNS` clause is a subset of interesting columns from the previous run.
- The metadata level in the LEVEL clause must be the same as the previous run.

If either of these two conditions is false, Drill will perform a full analysis over the entire table.

## General Information

- Drill 1.17 supports the Metastore and `ANALYZE TABLE` only for tables stored as Parquet files and only when stored in the `DFS` storage plugin.
- The first time you execute ANALYZE TABLE for a table, Drill will scan the entire table (all files.)
When you issue the same command for the next time, Drill will scan only those files added since the previous run.
The command will return the following message if table statistics are up-to-date:


```
apache drill (dfs.tmp)> analyze table lineitem refresh metadata;
+-------+---------------------------------------------------------+
|  ok   |                         summary                         |
+-------+---------------------------------------------------------+
| false | Table metadata is up to date, analyze wasn't performed. |
+-------+---------------------------------------------------------+
```

### Metadata usage

Drill uses the Metastore in several places. When you run a query with multiple directories, files or Parquet row groups,
 Drill will use statistics to "prune" the scan. That is, to identify those directories, files or row groups that
 do not contain data that your query needs. If you add new files or directories and do not rerun `ANALYZE TABLE`,
 then Drill will assume that existing metadata is invalid and wouldn't use it. Periodically rerun `ANALYZE TABLE` so
 that Drill can use table metadata when possible.

### Limitations

This feature is currently in the beta phase (preview, experimental) for Drill 1.17 and only applies to Parquet
 tables in this release. You must enable this feature through the `metastore.enabled` system/session option.

## Examples

Examples throughout this topic use the files and directories described in the following section, Directory, and File Setup.

### Directory and File Setup

Download [TPC-H sf1 tables](https://s3-us-west-1.amazonaws.com/drill-public/tpch/sf1/tpch_sf1_parquet.tar.gz) and
 unpack archive.

Create lineitem directory in `/tmp/` and two subdirectories under `/tmp/lineitem` named `s1` and `s2` and copy there table data:

    mkdir /tmp/lineitem
    mkdir /tmp/lineitem/s1
    mkdir /tmp/lineitem/s2
    cp TPCH/lineitem /tmp/lineitem/s1
    cp TPCH/lineitem /tmp/lineitem/s2

Query the directory `/tmp/lineitem`:

```
SELECT count(*) FROM dfs.tmp.lineitem;
+----------+
|  EXPR$0  |
+----------+
| 12002430 |
+----------+
1 row selected (0.291 seconds)
```

Notice that the query plan contains a group scan with `usedMetastore = false`:


```
00-00    Screen : rowType = RecordType(BIGINT EXPR$0): rowcount = 1.0, cumulative cost = {2.1 rows, 2.1 cpu, 1.0 io, 0.0 network, 0.0 memory}, id = 8410
00-01      Project(EXPR$0=[$0]) : rowType = RecordType(BIGINT EXPR$0): rowcount = 1.0, cumulative cost = {2.0 rows, 2.0 cpu, 1.0 io, 0.0 network, 0.0 memory}, id = 8409
00-02        DirectScan(groupscan=[selectionRoot = file:/tmp/lineitem, numFiles = 12, usedMetadataSummaryFile = false, usedMetastore = false, ...
```

### Computing and storing table metadata to Drill Metastore

Enable Drill Metastore:

```
SET `metastore.enabled` = true;
```

Run [ANALYZE TABLE]({{site.baseurl}}/docs/analyze-table-refresh-metadata) command on the table, whose metadata should
 be computed and stored into the Drill Metastore:


```
apache drill> ANALYZE TABLE dfs.tmp.lineitem REFRESH METADATA;
+------+-------------------------------------------------------------+
|  ok  |                           summary                           |
+------+-------------------------------------------------------------+
| true | Collected / refreshed metadata for table [dfs.tmp.lineitem] |
+------+-------------------------------------------------------------+
1 row selected (32.257 seconds)
```

The output of this command provides the status of the command execution and its summary.

Once, table metadata is collected and stored in the Metastore, it will be used when querying the table. To ensure that it was used, its
 info was added to the group scan (`usedMetastore=true` entry in `ParquetGroupScan`):


```
00-00    Screen : rowType = RecordType(BIGINT EXPR$0): rowcount = 1.0, cumulative cost = {2.1 rows, 2.1 cpu, 1.0 io, 0.0 network, 0.0 memory}, id = 8560
00-01      Project(EXPR$0=[$0]) : rowType = RecordType(BIGINT EXPR$0): rowcount = 1.0, cumulative cost = {2.0 rows, 2.0 cpu, 1.0 io, 0.0 network, 0.0 memory}, id = 8559
00-02        DirectScan(groupscan=[selectionRoot = /tmp/lineitem, numFiles = 12, usedMetadataSummaryFile = false, usedMetastore = true, ...
```

### Performing incremental analysis

Rerun [ANALYZE TABLE]({{site.baseurl}}/docs/analyze-table-refresh-metadata) command on the table with previously collected metadata:


```
apache drill> ANALYZE TABLE dfs.tmp.lineitem REFRESH METADATA;
+-------+---------------------------------------------------------+
|  ok   |                         summary                         |
+-------+---------------------------------------------------------+
| false | Table metadata is up to date, analyze wasn't performed. |
+-------+---------------------------------------------------------+
1 row selected (0.249 seconds)
```

### Exposing Drill Metastore metadata through `INFORMATION_SCHEMA` tables

Drill exposes some Metastore tables metadata through `INFORMATION_SCHEMA` tables.
Note, that Metastore metadata will be exposed to the info schema, only if Metastore is enabled. If it is disabled, info
 tables won't contain Metastore metadata.

`TABLES` table includes the set of tables on which you have run `ANALYZE TABLE`.
Description of Metastore-specific columns:

|Column name            |Type       |Nullable   |Description                                                                                            |
|-----------------------|-----------|-----------|-------------------------------------------------------------------------------------------------------|
|`TABLE_SOURCE`         |VARCHAR    |YES        |Table data type: `PARQUET`, `CSV`, `JSON`                                                              |
|`LOCATION`             |VARCHAR    |YES        |Table location: `/tmp/nation`                                                                          |
|`NUM_ROWS`             |BIGINT     |YES        |The total number of rows in all files of the table. Null if not known                                  |
|`LAST_MODIFIED_TIME`   |TIMESTAMP  |YES        |Timestamp of the most-recently modified file within the table. Updated on each `ANALYZE TABLE` run.    |

Example of its content:

```
apache drill> SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME='lineitem';
+---------------+--------------+------------+------------+--------------+---------------+----------+-----------------------+
| TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME | TABLE_TYPE | TABLE_SOURCE |   LOCATION    | NUM_ROWS |  LAST_MODIFIED_TIME   |
+---------------+--------------+------------+------------+--------------+---------------+----------+-----------------------+
| DRILL         | dfs.tmp      | lineitem   | TABLE      | PARQUET      | /tmp/lineitem | 12002430 | 2016-09-28 03:22:58.0 |
+---------------+--------------+------------+------------+--------------+---------------+----------+-----------------------+
1 row selected (0.157 seconds)
```

The `COLUMNS` table describes the columns within each table. Only those columns listed in the `COLUMNS` clause of the
 `ANALYZE TABLE` statement appear in this table.

|Column name            |Type       |Nullable   |Description                                                                                                                                                                        |
|-----------------------|-----------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`COLUMN_DEFAULT`       |VARCHAR    |YES        |Column default value.                                                                                                                                                              |
|`COLUMN_FORMAT`        |VARCHAR    |YES        |Usually applicable for date time columns: `yyyy-MM-dd`. See [Format for Date, Time Conversion]({{site.baseurl}}/docs/create-or-replace-schema/#format-for-date-time-conversion).   |
|`NUM_NULLS`            |BIGINT     |YES        |The number of rows which contain nulls for this column.                                                                                                                            |
|`MIN_VAL`              |VARCHAR    |YES        |The minimum value of the column expressed as a string. For example: `'-273'`.                                                                                                      |
|`MAX_VAL`              |VARCHAR    |YES        |The maximum value of the column expressed as a string. For example: `'100500'`.                                                                                                    |
|`NDV`                  |FLOAT8     |YES        |Number of distinct values in column, expressed in Double.                                                                                                                          |
|`EST_NUM_NON_NULLS`    |FLOAT8     |YES        |Estimated number of non null values, expressed in Double.                                                                                                                          |
|`IS_NESTED`            |BIT        |NO         |If column is nested. Nested columns are extracted from columns with struct type.                                                                                                   |

Example of its content:

```
apache drill> SELECT * FROM INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_NAME='lineitem';
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+--------------+---------------------------------------------+-----------+-------------------+-----------+
| TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME |   COLUMN_NAME   | ORDINAL_POSITION | COLUMN_DEFAULT | IS_NULLABLE |     DATA_TYPE     | CHARACTER_MAXIMUM_LENGTH | CHARACTER_OCTET_LENGTH | NUMERIC_PRECISION | NUMERIC_PRECISION_RADIX | NUMERIC_SCALE | DATETIME_PRECISION | INTERVAL_TYPE | INTERVAL_PRECISION | COLUMN_SIZE | COLUMN_FORMAT | NUM_NULLS |   MIN_VAL    |                   MAX_VAL                   |    NDV    | EST_NUM_NON_NULLS | IS_NESTED |
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+--------------+---------------------------------------------+-----------+-------------------+-----------+
| DRILL         | dfs.tmp      | lineitem   | dir0            | 1                | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | 0         | s1           | s2                                          | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_orderkey      | 2                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | 0         | 1            | 6000000                                     | 1499876.0 | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_partkey       | 3                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | 0         | 1            | 200000                                      | 199857.0  | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_suppkey       | 4                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | 0         | 1            | 10000                                       | 10001.0   | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_linenumber    | 5                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | 0         | 1            | 7                                           | 7.0       | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_quantity      | 6                | null           | YES         | DOUBLE            | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 24          | null          | 0         | 1.0          | 50.0                                        | 50.0      | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_extendedprice | 7                | null           | YES         | DOUBLE            | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 24          | null          | 0         | 901.0        | 104949.5                                    | 933142.0  | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_discount      | 8                | null           | YES         | DOUBLE            | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 24          | null          | 0         | 0.0          | 0.1                                         | 11.0      | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_tax           | 9                | null           | YES         | DOUBLE            | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 24          | null          | 0         | 0.0          | 0.08                                        | 9.0       | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_returnflag    | 10               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | 0         | A            | R                                           | 3.0       | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_linestatus    | 11               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | 0         | F            | O                                           | 2.0       | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_shipdate      | 12               | null           | YES         | DATE              | null                     | null                   | null              | null                    | null          | 10                 | null          | null               | 10          | null          | 0         | 694310400000 | 912470400000                                | 2526.0    | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_commitdate    | 13               | null           | YES         | DATE              | null                     | null                   | null              | null                    | null          | 10                 | null          | null               | 10          | null          | 0         | 696816000000 | 909792000000                                | 2466.0    | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_receiptdate   | 14               | null           | YES         | DATE              | null                     | null                   | null              | null                    | null          | 10                 | null          | null               | 10          | null          | 0         | 694483200000 | 915062400000                                | 2554.0    | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_shipinstruct  | 15               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | 0         | COLLECT COD  | TAKE BACK RETURN                            | 4.0       | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_shipmode      | 16               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | 0         | AIR          | TRUCK                                       | 7.0       | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_comment       | 17               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | 0         |  Tiresias    | zzle? slyly final platelets sleep quickly.  | 4586320.0 | 1.200243E7        | false     |
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+--------------+---------------------------------------------+-----------+-------------------+-----------+
17 rows selected (0.187 seconds)
```

A table can be divided into directories, called "partitions". The `PARTITIONS` table contains an entry for each directory.

|Column name            |Type       |Nullable   |Description                                                                                                                                                                        |
|-----------------------|-----------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`TABLE_CATALOG`        |VARCHAR    |YES        |Table catalog (currently we have only one catalog): `DRILL`.                                                                                                                       |
|`TABLE_SCHEMA`         |VARCHAR    |YES        |Table schema: `dfs.tmp`.                                                                                                                                                           |
|`TABLE_NAME`           |VARCHAR    |YES        |Table name: `nation`.                                                                                                                                                              |
|`METADATA_KEY`         |VARCHAR    |YES        |Top level segment key, the same for all nested segments and partitions: `part_int=3`.                                                                                              |
|`METADATA_TYPE`        |VARCHAR    |YES        |`SEGMENT` or `PARTITION`. Partition here corresponds to "Drill partition", though segment corresponds to data parts like partitions in general case, for example, Hive partition.  |
|`METADATA_IDENTIFIER`  |VARCHAR    |YES        |Current metadata identifier: `part_int=3/part_varchar=g`. It is unique value for segment or partition within the table.                                                            |
|`PARTITION_COLUMN`     |VARCHAR    |YES        |Partition column name: `part_varchar`.                                                                                                                                             |
|`PARTITION_VALUE`      |VARCHAR    |YES        |Partition column value: `g`.                                                                                                                                                       |
|`LOCATION`             |VARCHAR    |YES        |Segment location, `null` for partitions: `/tmp/nation/part_int=3`.                                                                                                                 |
|`LAST_MODIFIED_TIME`   |TIMESTAMP  |YES        |Last modification time.                                                                                                                                                            |

Example of its content:

```
apache drill (information_schema)> SELECT * FROM INFORMATION_SCHEMA.`PARTITIONS` WHERE TABLE_NAME='lineitem';
+---------------+--------------+------------+--------------+---------------+---------------------+------------------+-----------------+------------------+-----------------------+
| TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME | METADATA_KEY | METADATA_TYPE | METADATA_IDENTIFIER | PARTITION_COLUMN | PARTITION_VALUE |     LOCATION     |  LAST_MODIFIED_TIME   |
+---------------+--------------+------------+--------------+---------------+---------------------+------------------+-----------------+------------------+-----------------------+
| DRILL         | dfs.tmp      | lineitem   | s2           | SEGMENT       | s2                  | `dir0`           | s2              | /tmp/lineitem/s2 | 2016-09-28 03:22:58.0 |
| DRILL         | dfs.tmp      | lineitem   | s1           | SEGMENT       | s1                  | `dir0`           | s1              | /tmp/lineitem/s1 | 2016-09-28 03:22:58.0 |
+---------------+--------------+------------+--------------+---------------+---------------------+------------------+-----------------+------------------+-----------------------+
2 rows selected (0.149 seconds)
```

### Dropping table metadata

Table metadata may be dropped using `ANALYZE TABLE DROP METADATA` command:

```
apache drill> ANALYZE TABLE dfs.tmp.lineitem DROP METADATA;
+------+----------------------------------------+
|  ok  |                summary                 |
+------+----------------------------------------+
| true | Metadata for table [lineitem] dropped. |
+------+----------------------------------------+
1 row selected (0.291 seconds)
```

### Collecting metadata for specific set of columns

For the case when metadata for several columns should be computed and stored into the Metastore, the following command may be used:

```
apache drill (information_schema)> ANALYZE TABLE dfs.tmp.lineitem COLUMNS(l_orderkey, l_partkey) REFRESH METADATA;
+------+-------------------------------------------------------------+
|  ok  |                           summary                           |
+------+-------------------------------------------------------------+
| true | Collected / refreshed metadata for table [dfs.tmp.lineitem] |
+------+-------------------------------------------------------------+
1 row selected (94.1 seconds)
```

Now, check, that metadata is collected only for specified columns (`MIN_VAL`, `MAX_VAL`, `NDV`, etc.), but all
 columns are present:

```
apache drill (information_schema)> SELECT * FROM INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_NAME='lineitem';
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+---------+---------+-----------+-------------------+-----------+
| TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME |   COLUMN_NAME   | ORDINAL_POSITION | COLUMN_DEFAULT | IS_NULLABLE |     DATA_TYPE     | CHARACTER_MAXIMUM_LENGTH | CHARACTER_OCTET_LENGTH | NUMERIC_PRECISION | NUMERIC_PRECISION_RADIX | NUMERIC_SCALE | DATETIME_PRECISION | INTERVAL_TYPE | INTERVAL_PRECISION | COLUMN_SIZE | COLUMN_FORMAT | NUM_NULLS | MIN_VAL | MAX_VAL |    NDV    | EST_NUM_NON_NULLS | IS_NESTED |
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+---------+---------+-----------+-------------------+-----------+
| DRILL         | dfs.tmp      | lineitem   | dir0            | 1                | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | 0         | s1      | s2      | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_orderkey      | 2                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | 0         | 1       | 6000000 | 1499876.0 | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_partkey       | 3                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | 0         | 1       | 200000  | 199857.0  | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_suppkey       | 4                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_linenumber    | 5                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_quantity      | 6                | null           | YES         | DOUBLE            | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 24          | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_extendedprice | 7                | null           | YES         | DOUBLE            | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 24          | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_discount      | 8                | null           | YES         | DOUBLE            | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 24          | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_tax           | 9                | null           | YES         | DOUBLE            | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 24          | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_returnflag    | 10               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_linestatus    | 11               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_shipdate      | 12               | null           | YES         | DATE              | null                     | null                   | null              | null                    | null          | 10                 | null          | null               | 10          | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_commitdate    | 13               | null           | YES         | DATE              | null                     | null                   | null              | null                    | null          | 10                 | null          | null               | 10          | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_receiptdate   | 14               | null           | YES         | DATE              | null                     | null                   | null              | null                    | null          | 10                 | null          | null               | 10          | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_shipinstruct  | 15               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_shipmode      | 16               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | null      | null    | null    | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_comment       | 17               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | null      | null    | null    | null      | null              | false     |
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+---------+---------+-----------+-------------------+-----------+
17 rows selected (0.183 seconds)
```
