---
title: "Using Drill Metastore"
parent: "Drill Metastore"
date: 2020-03-17
---

Drill 1.17 introduces the Drill Metastore which stores the table schema and table statistics. Statistics allow Drill to better create optimal query plans.

The Metastore is a beta feature and is subject to change.
In particular, the SQL commands and Metastore format may change based on your experience and feedback.
{% include startnote.html %}
In Drill 1.17, Metastore supports only tables in Parquet format. The feature is disabled by default.
In Drill 1.18, Metastore supports all format plugins (except MaprDB) for the file system plugin. The feature is still disabled by default.
{% include endnote.html %}

## Drill Metastore introduction

One of the main advantages of Drill is schema-on-read. But Drill can’t handle some cases with this approach, there
 are the issues related to schema evolution or ambiguous schema.

Significant benefits of schema-aware execution:

 - At Planning time:
    - Better scope for planning optimizations.
    - Proper estimation of column widths since types are known, hence more accurate costing.
    - Graceful early exit if certain data type validations fail.
 - At Runtime:
    - `SchemaChange` exceptions avoidance. All minor fragments will have a common understanding of the schema.

Reading the data along with its statistics metadata helps to build more efficient plans and optimize query execution:

 - Crucial for optimal join planning, 2-phase aggregation vs 1-phase aggregation planning, selectivity estimation of filter conditions, parallelization decisions.

Taking into account the above points, existing query processing can be improved by:

 - storing table schema and reusing it;
 - collecting, storing and reusing table statistics to improve query planning.

One of the main steps to resolve all these goals is providing the framework for metadata management named hereafter
 as Drill Metastore.

## Enabling Drill Metastore

To use the Drill Metastore, you must enable it at the session or system level with one of the following commands:

	SET `metastore.enabled` = true;
	ALTER SYSTEM SET `metastore.enabled` = true;

Alternatively, you can enable the option in the Drill Web UI at `http://<drill-hostname-or-ip-address>:8047/options`.

## Computing and storing table metadata to Drill Metastore

Once you enable the Metastore, the next step is to populate it with data. Metastore entries are optional. If you
 query a table without a Metastore entry, Drill works with that table just as if the Metastore was disabled. In Drill,
 only add data to the Metastore when doing so improves query performance. In general, large tables benefit from
 statistics more than small tables do.

Unlike Hive, Drill does not require you to declare a schema. Instead, Drill infers the schema by scanning your table 
 in the same way as it is done during regular select and computes some metadata like `MIN` / `MAX` column values and
 `NULLS_COUNT` designated as "metadata" to be able to produce more optimizations like filter push-down, etc. If
 `planner.statistics.use` option is enabled, this command will also calculate and store table statistics into Drill
 Metastore.

## Configuration

To configure the Metastore, create `$DRILL_HOME/conf/drill-metastore-override.conf` file.
 This is a HOCON-format file, just like `drill-override.conf`. All Metastore configuration properties should reside in
 `drill.metastore` namespace.

### Metastore Implementations

Drill Metastore offers an API that allows for any number of implementations. See
 [metastore-api module docs](https://github.com/apache/drill/blob/master/metastore/metastore-api/README.md) for a description of the API.

The default implementation is the [Iceberg Metastore]({{site.baseurl}}/docs/drill-iceberg-metastore) based on
 [Iceberg tables](http://iceberg.incubator.apache.org) that provides support of transactions and concurrent writes. It
 resides on the file system specified in Metastore configuration.

To specify custom Metastore implementation, place the JAR which has the implementation of
 `org.apache.drill.metastore.Metastore` interface into classpath and indicate custom class in the
 `drill.metastore.implementation.class` config property.
The default value is the following:

```
drill.metastore: {
  implementation.class: "org.apache.drill.metastore.iceberg.IcebergMetastore"
}
```

### Metastore Components

The Drill 1.17 version of the Metastore stores metadata about tables: the table schema and table statistics.
The Metastore is an active subproject of Drill, See [DRILL-6552](https://issues.apache.org/jira/browse/DRILL-6552) for more information.

### Table Metadata

Table Metadata includes the following info:

 - Table schema, column name, type, nullability, scale and precision if available, and other info. For details please
  refer to [Schema provisioning]({{site.baseurl}}/docs/create-or-replace-schema/#usage-notes).
 - Table statistics are of two kinds:
    - Summary statistics: `MIN`, `MAX`, `NULL count`, etc.
    - Detail statistics: histograms, `NDV`, etc.

Schema information and summary statistics also computed and stored for table segments, files, row groups and partitions.

The detailed metadata schema is described [here](https://github.com/apache/drill/tree/master/metastore/metastore-api#metastore-tables).
You can try out the metadata to get a sense of what is available, by using the
 [Inspect the Metastore using `INFORMATION_SCHEMA` tables](#inspect-the-metastore-using-information_schema-tables) tutorial.

Every table described by the Metastore may be a bare file or one or more files that reside in one or more directories.

If a table consists of a single directory or file, then it is non-partitioned. The single directory can contain any number of files.
Larger tables tend to have subdirectories. Each subdirectory is a partition and such a table are called "partitioned".
Please refer to [Exposing Drill Metastore metadata through `INFORMATION_SCHEMA` tables](#exposing-drill-metastore-metadata-through-information_schema-tables)
 for information, how to query partitions and segments metadata.

A traditional database divides tables into schemas and tables.
Drill can connect to any number of data sources, each of which may have its own schema.
As a result, the Metastore labels tables with a combination of (plugin configuration name, workspace name, table name).
Note that if before renaming any of these items, you must delete table's Metadata entry and recreate it after renaming.

### Using schema provisioning feature with Drill Metastore

The Drill Metastore holds both schema and statistics information for a table. The `ANALYZE` command can infer the table
 schema for well-defined tables (such as many Parquet tables). Some tables are too complex or variable for Drill's
 schema inference to work well. For example, JSON tables often omit fields or have long runs of nulls so that Drill
 cannot determine column types. In these cases, you can specify the correct schema based on your knowledge of the
 table's structure. You specify a schema in the `ANALYZE` command using the 
 [Schema provisioning]({{site.baseurl}}/docs/plugin-configuration-basics/#specifying-the-schema-as-table-function-parameter) syntax.

Please refer to [Provisioning schema for Drill Metastore](#provisioning-schema-for-drill-metastore) for examples of usage.

### Schema priority

Drill uses metadata during both query planning and execution. Drill gives you multiple ways to provide a schema.

When you run the `ANALYZE TABLE` command, Drill will use the following rules for the table schema to be stored in the Metastore. In priority order:

* A schema provided in the table function.
* A schema file, created with `CREATE OR REPLACE SCHEMA`, in the table root directory.
* Schema inferred from file data.

To plan a query, Drill requires information about your file partitions (if any) and about row and column cardinality.
Drill does not use the provided schema for planning as it does not provide this metadata. Instead, at plan time Drill
obtains metadata from one of the following, again in priority order:

* The Drill Metastore, if available.
* Inferred from file data. Drill scans the table's directory structure to identify partitions.
 Drill estimates row counts based on the file size. Drill uses default estimates for column cardinality.

At query execution time, a schema tells Drill the shape of your data and how that data should be converted to Drill's SQL types.
Your choices for execution-time schema, in priority order, are:

* With a table function:
   - specify an inline schema
   - specify the path to the schema file.
* With a schema file, created with `CREATE OR REPLACE SCHEMA`, in the table root directory.
* Using the schema from the Drill Metastore, if available.
* Infer the schema directly from file data.

### Related Session/System Options

The Metastore provides a number of options to fit your environment. The default options are fine in most cases.
The options are set via `ALTER SYSTEM SET`, `SET` (it is an alias for `ALTER SESSION SET`) or the Drill Web console.

In general, admin should set the options via `ALTER SYSTEM` so that they take effect for all users.
Setting options at the session level is an advanced topic.

- **metastore.enabled**
Enables Drill Metastore usage to be able to store table metadata during ANALYZE TABLE commands execution and to be able
 to read table metadata during regular queries execution or when querying some INFORMATION_SCHEMA tables. Default is `false`.
- **metastore.metadata.store.depth_level**
Specifies the most-specific metadata kind to be collected with more general metadata kinds. Same options as the _level_ option above. Default is `'ALL'`.
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
Enables the Drill query planner to use table and column statistics stored in the Metastore. Default is `true`.
Enable `planner.statistics.use` to be able to use statistics during query planning.
- **drill.exec.storage.implicit.last_modified_time.column.label**
Sets the implicit column name for the last modified time (`lmt`) column. Used when producing Metastore analyze. You can
 set the last modified time column name to custom name when current column name clashes which column name present in the
 table. If your table contains a column name with the same name as an implicit column, the implicit column takes
 priority and shadows column from the table. Default is `lmt`.
- **drill.exec.storage.implicit.row_group_index.column.label**
Sets the implicit column name for the row group index (`rgi`) column. Used when producing Metastore analyze. You can
 set row group index column name to custom name when current column name clashes which column name present in the
 table. If your table contains a column name with the same name as an implicit column, the implicit column takes
 priority and shadows column from the table. Default is `rgi`.
- **drill.exec.storage.implicit.row_group_length.column.label**
Sets the implicit column name for the row group length (`rgl`) column. Used when producing Metastore analyze. You can
 set row group length column name to custom name when current column name clashes which column name present in the
 table. If your table contains a column name with the same name as an implicit column, the implicit column takes
 priority and shadows column from the table. Default is `rgl`.
- **drill.exec.storage.implicit.row_group_start.column.label**
Sets the implicit column name for the row group start (`rgs`) column. Used when producing Metastore analyze. You can
 set row group start column name to custom name when current column name clashes which column name present in the
 table. If your table contains a column name with the same name as an implicit column, the implicit column takes
 priority and shadows column from the table. Default is `rgs`.

## Analyzing a table

You create Metastore metadata by running the [`ANALYZE TABLE`]({{site.baseurl}}/docs/analyze-table-refresh-metadata) command.
The first time you run it, the Metastore will infer the schema and (depending on which options you have selected), populate statistics.

Tables change over time. To keep the Metastore metadata up-to-date, you must periodically run `ANALYZE TABLE` again
 on each changed table.
When you do `ANALYZE TABLE` a second time, Drill will attempt to update statistics, called "incremental analysis".

Incremental analysis will compute metadata only for files and partitions changed since the last analysis and reuse
 actual metadata from the Metastore where possible.

The command will return the following message if table statistics are up-to-date:

```
ANALYZE TABLE `lineitem` REFRESH METADATA;
+-------+---------------------------------------------------------+
|  ok   |                         summary                         |
+-------+---------------------------------------------------------+
| false | Table metadata is up to date, analyze wasn't performed. |
+-------+---------------------------------------------------------+
```

Table schemas evolve over time. If your table adds (or removes) columns, run `ANALYZE TABLE` with the new set of
 columns. Drill will perform a full table analysis.

## General Information

### Metadata usage

Drill uses the Metastore in several places. When you run a query with multiple directories, files or Parquet row groups,
 Drill will use statistics to "prune" the scan. That is, to identify those directories, files or row groups that
 do not contain data that your query needs. If you add new files or directories and do not rerun `ANALYZE TABLE`,
 then Drill will assume that existing metadata is invalid and wouldn't use it. Periodically rerun `ANALYZE TABLE` so
 that Drill can use table metadata when possible.

### Exposing Drill Metastore metadata through `INFORMATION_SCHEMA` tables

Drill exposes some Metastore tables metadata through `INFORMATION_SCHEMA` tables.
Note, that Metastore metadata will be exposed to the `INFORMATION_SCHEMA` only if Metastore is enabled. If it is disabled, info
 tables won't contain Metastore metadata.

`TABLES` table includes the set of tables on which you have run `ANALYZE TABLE`.
Description of Metastore-specific columns:

|Column name            |Type       |Nullable   |Description                                                                                            |
|-----------------------|-----------|-----------|-------------------------------------------------------------------------------------------------------|
|`TABLE_SOURCE`         |VARCHAR    |YES        |Table data type: `PARQUET`, `CSV`, `JSON`                                                              |
|`LOCATION`             |VARCHAR    |YES        |Table location: `/tmp/nation`                                                                          |
|`NUM_ROWS`             |BIGINT     |YES        |Total number of rows in all files of the table. Null if not known                                      |
|`LAST_MODIFIED_TIME`   |TIMESTAMP  |YES        |Timestamp of the most-recently modified file within the table. Updated on each `ANALYZE TABLE` run.    |

The `COLUMNS` table describes the columns within each table. Only those columns listed in the `COLUMNS` clause of the
 `ANALYZE TABLE` statement appear in this table.

|Column name            |Type       |Nullable   |Description                                                                                                                                                                        |
|-----------------------|-----------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`COLUMN_DEFAULT`       |VARCHAR    |YES        |Column default value.                                                                                                                                                              |
|`COLUMN_FORMAT`        |VARCHAR    |YES        |Usually applicable for date time columns: `yyyy-MM-dd`. See [Format for Date, Time Conversion]({{site.baseurl}}/docs/create-or-replace-schema/#format-for-date-time-conversion).   |
|`NUM_NULLS`            |BIGINT     |YES        |Number of rows which contain nulls for this column.                                                                                                                                |
|`MIN_VAL`              |VARCHAR    |YES        |Minimum value of the column. For example: `'-273'`.                                                                                                                                |
|`MAX_VAL`              |VARCHAR    |YES        |Maximum value of the column. For example: `'100500'`.                                                                                                                              |
|`NDV`                  |FLOAT8     |YES        |Number of distinct values in column.                                                                                                                                               |
|`EST_NUM_NON_NULLS`    |FLOAT8     |YES        |Estimated number of non null values.                                                                                                                                               |
|`IS_NESTED`            |BIT        |NO         |If column is nested. Nested columns are extracted from columns with struct type.                                                                                                   |

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

### Limitations of the 1.17 release

 - Applies to tables stored as Parquet files and only when stored in the `DFS` storage plugin.
 - Disabled by default. You must enable this feature through the `metastore.enabled` system/session option.

### Limitations of the 1.18 release
 - Applies to all file system storage plugin formats except for MaprDB.

### Cheat sheet of `ANALYZE TABLE` commands

 - Add a new table with `ANALYZE TABLE dfs.tmp.lineitem REFRESH METADATA` command.
 - When table data (but not schema) changes, run `ANALYZE TABLE dfs.tmp.lineitem REFRESH METADATA` command.
 - When the table schema changes, run `ANALYZE TABLE dfs.tmp.lineitem COLUMNS (col1, col2, ...) REFRESH METADATA` command.
 - If partitions are added or removed, run `ANALYZE TABLE dfs.tmp.lineitem REFRESH METADATA` command.
 - Remove table metadata by submitting `ANALYZE TABLE dfs.tmp.lineitem DROP METADATA` command.

## Tutorial

Examples throughout this topic use the files and directories described in the following section `Directory and File Setup`.

### Directory and File Setup

The following examples are written for local file system, but Drill Metastore supports collecting metadata for tables
 placed in any any of Drill's supported file systems. The examples work for both embedded and distributed Drill modes.

Download [TPC-H sf1 tables](https://s3-us-west-1.amazonaws.com/drill-public/tpch/sf1/tpch_sf1_parquet.tar.gz) and
 unpack archive to desired file system.

Set up storage plugin for desired file system, as described here:
 [Connecting Drill to a File System]({{site.baseurl}}/docs/file-system-storage-plugin/#connecting-drill-to-a-file-system).

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

### Compute table metadata and store in the Drill Metastore

Enable Drill Metastore:

```
SET `metastore.enabled` = true;
```

The above command enables the Metastore for just this one session.

Run the [ANALYZE TABLE]({{site.baseurl}}/docs/analyze-table-refresh-metadata) command on the table, whose metadata should
 be computed and stored into the Drill Metastore:

```
ANALYZE TABLE dfs.tmp.lineitem REFRESH METADATA;
+------+-------------------------------------------------------------+
|  ok  |                           summary                           |
+------+-------------------------------------------------------------+
| true | Collected / refreshed metadata for table [dfs.tmp.lineitem] |
+------+-------------------------------------------------------------+
1 row selected (32.257 seconds)
```

The output of this command provides the status of the command execution and its summary.

Now that we've collected table metadata, we can use it when we query the table, by checking the `usedMetastore=true` entry in `ParquetGroupScan`:

```
00-00    Screen : rowType = RecordType(BIGINT EXPR$0): rowcount = 1.0, cumulative cost = {2.1 rows, 2.1 cpu, 1.0 io, 0.0 network, 0.0 memory}, id = 8560
00-01      Project(EXPR$0=[$0]) : rowType = RecordType(BIGINT EXPR$0): rowcount = 1.0, cumulative cost = {2.0 rows, 2.0 cpu, 1.0 io, 0.0 network, 0.0 memory}, id = 8559
00-02        DirectScan(groupscan=[selectionRoot = /tmp/lineitem, numFiles = 12, usedMetadataSummaryFile = false, usedMetastore = true, ...
```

### Perform incremental analysis

Rerun [ANALYZE TABLE]({{site.baseurl}}/docs/analyze-table-refresh-metadata) command on the `lineitem` table:

```
ANALYZE TABLE dfs.tmp.lineitem REFRESH METADATA;
+-------+---------------------------------------------------------+
|  ok   |                         summary                         |
+-------+---------------------------------------------------------+
| false | Table metadata is up to date, analyze wasn't performed. |
+-------+---------------------------------------------------------+
1 row selected (0.249 seconds)
```

### Inspect the Metastore using INFORMATION_SCHEMA tables

Run the following query to inspect `lineitem` table metadata from `TABLES` table stored in the Metastore:

```
SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME='lineitem';
+---------------+--------------+------------+------------+--------------+---------------+----------+-----------------------+
| TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME | TABLE_TYPE | TABLE_SOURCE |   LOCATION    | NUM_ROWS |  LAST_MODIFIED_TIME   |
+---------------+--------------+------------+------------+--------------+---------------+----------+-----------------------+
| DRILL         | dfs.tmp      | lineitem   | TABLE      | PARQUET      | /tmp/lineitem | 12002430 | 2016-09-28 03:22:58.0 |
+---------------+--------------+------------+------------+--------------+---------------+----------+-----------------------+
1 row selected (0.157 seconds)
```

To obtain columns with their types and descriptions within the `lineitem` table, run the following query:

```
SELECT * FROM INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_NAME='lineitem';
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+--------------+---------------------------------------------+-----------+-------------------+-----------+
| TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME |   COLUMN_NAME   | ORDINAL_POSITION | COLUMN_DEFAULT | IS_NULLABLE |     DATA_TYPE     | CHARACTER_MAXIMUM_LENGTH | CHARACTER_OCTET_LENGTH | NUMERIC_PRECISION | NUMERIC_PRECISION_RADIX | NUMERIC_SCALE | DATETIME_PRECISION | INTERVAL_TYPE | INTERVAL_PRECISION | COLUMN_SIZE | COLUMN_FORMAT | NUM_NULLS |   MIN_VAL    |                   MAX_VAL                   |    NDV    | EST_NUM_NON_NULLS | IS_NESTED |
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+--------------+---------------------------------------------+-----------+-------------------+-----------+
| DRILL         | dfs.tmp      | lineitem   | dir0            | 1                | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | 0         | s1           | s2                                          | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_orderkey      | 2                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | 0         | 1            | 6000000                                     | 1499876.0 | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_partkey       | 3                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | 0         | 1            | 200000                                      | 199857.0  | 1.200243E7        | false     |
...
| DRILL         | dfs.tmp      | lineitem   | l_comment       | 17               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | 0         |  Tiresias    | zzle? slyly final platelets sleep quickly.  | 4586320.0 | 1.200243E7        | false     |
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+--------------+---------------------------------------------+-----------+-------------------+-----------+
17 rows selected (0.187 seconds)
```

The sample `lineitem` table has two partitions. The `PARTITIONS` table contains an entry for each directory:

```
SELECT * FROM INFORMATION_SCHEMA.`PARTITIONS` WHERE TABLE_NAME='lineitem';
+---------------+--------------+------------+--------------+---------------+---------------------+------------------+-----------------+------------------+-----------------------+
| TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME | METADATA_KEY | METADATA_TYPE | METADATA_IDENTIFIER | PARTITION_COLUMN | PARTITION_VALUE |     LOCATION     |  LAST_MODIFIED_TIME   |
+---------------+--------------+------------+--------------+---------------+---------------------+------------------+-----------------+------------------+-----------------------+
| DRILL         | dfs.tmp      | lineitem   | s2           | SEGMENT       | s2                  | `dir0`           | s2              | /tmp/lineitem/s2 | 2016-09-28 03:22:58.0 |
| DRILL         | dfs.tmp      | lineitem   | s1           | SEGMENT       | s1                  | `dir0`           | s1              | /tmp/lineitem/s1 | 2016-09-28 03:22:58.0 |
+---------------+--------------+------------+--------------+---------------+---------------------+------------------+-----------------+------------------+-----------------------+
2 rows selected (0.149 seconds)
```

### Drop table metadata

Once we are done exploring metadata we can drop the metadata for the `lineitem` table.

Table metadata may be dropped using `ANALYZE TABLE DROP METADATA` command:

```
ANALYZE TABLE dfs.tmp.lineitem DROP METADATA;
+------+----------------------------------------+
|  ok  |                summary                 |
+------+----------------------------------------+
| true | Metadata for table [lineitem] dropped. |
+------+----------------------------------------+
1 row selected (0.291 seconds)
```

### Collect metadata for specific set of columns

Next let's gather metadata for a subset of the columns in the `lineitem` table. You would do this to allow
 Drill to optimize `WHERE` conditions on certain columns. Also, if file size or the number of columns grows large, it
 can take too long to gather all statistics. Instead you can speed up analysis by gathering statistics only for
 selected columns: those actually used in the `WHERE` clause.

For the case when metadata for several columns should be computed and stored into the Metastore, the following command may be used:

```
ANALYZE TABLE dfs.tmp.lineitem COLUMNS(l_orderkey, l_partkey) REFRESH METADATA;
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
SELECT * FROM INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_NAME='lineitem';
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+---------+---------+-----------+-------------------+-----------+
| TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME |   COLUMN_NAME   | ORDINAL_POSITION | COLUMN_DEFAULT | IS_NULLABLE |     DATA_TYPE     | CHARACTER_MAXIMUM_LENGTH | CHARACTER_OCTET_LENGTH | NUMERIC_PRECISION | NUMERIC_PRECISION_RADIX | NUMERIC_SCALE | DATETIME_PRECISION | INTERVAL_TYPE | INTERVAL_PRECISION | COLUMN_SIZE | COLUMN_FORMAT | NUM_NULLS | MIN_VAL | MAX_VAL |    NDV    | EST_NUM_NON_NULLS | IS_NESTED |
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+---------+---------+-----------+-------------------+-----------+
| DRILL         | dfs.tmp      | lineitem   | dir0            | 1                | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | 0         | s1      | s2      | null      | null              | false     |
| DRILL         | dfs.tmp      | lineitem   | l_orderkey      | 2                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | 0         | 1       | 6000000 | 1499876.0 | 1.200243E7        | false     |
| DRILL         | dfs.tmp      | lineitem   | l_partkey       | 3                | null           | YES         | INTEGER           | null                     | null                   | 0                 | 2                       | 0             | null               | null          | null               | 11          | null          | 0         | 1       | 200000  | 199857.0  | 1.200243E7        | false     |
...
| DRILL         | dfs.tmp      | lineitem   | l_comment       | 17               | null           | YES         | CHARACTER VARYING | 65535                    | 65535                  | null              | null                    | null          | null               | null          | null               | 65535       | null          | null      | null    | null    | null      | null              | false     |
+---------------+--------------+------------+-----------------+------------------+----------------+-------------+-------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+--------------------+-------------+---------------+-----------+---------+---------+-----------+-------------------+-----------+
17 rows selected (0.183 seconds)
```

### Provisioning schema for Drill Metastore

#### Directory and File Setup

Ensure you have configured the file system storage plugin as described here:
 [Connecting Drill to a File System]({{site.baseurl}}/docs/file-system-storage-plugin/#connecting-drill-to-a-file-system).

Set `store.format` to `csvh`:

```
SET `store.format`='csvh';
+------+-----------------------+
|  ok  |        summary        |
+------+-----------------------+
| true | store.format updated. |
+------+-----------------------+
```

Create a text table based on the sample `/tpch/nation.parquet` table from `cp` plugin:

```
CREATE TABLE dfs.tmp.text_nation AS
  (SELECT *
   FROM cp.`/tpch/nation.parquet`);
+----------+---------------------------+
| Fragment | Number of records written |
+----------+---------------------------+
| 0_0      | 25                        |
+----------+---------------------------+
```

Query the table `text_nation`:

```
SELECT typeof(n_nationkey),
       typeof(n_name),
       typeof(n_regionkey),
       typeof(n_comment)
FROM dfs.tmp.text_nation
LIMIT 1;
+---------+---------+---------+---------+
| EXPR$0  | EXPR$1  | EXPR$2  | EXPR$3  |
+---------+---------+---------+---------+
| VARCHAR | VARCHAR | VARCHAR | VARCHAR |
+---------+---------+---------+---------+
```

Notice that the query plan contains a group scan with `usedMetastore = false`:

```
00-00    Screen : rowType = RecordType(ANY EXPR$0, ANY EXPR$1, ANY EXPR$2, ANY EXPR$3): rowcount = 1.0, cumulative cost = {25.1 rows, 109.1 cpu, 2247.0 io, 0.0 network, 0.0 memory}, id = 160
00-01      Project(EXPR$0=[TYPEOF($0)], EXPR$1=[TYPEOF($1)], EXPR$2=[TYPEOF($2)], EXPR$3=[TYPEOF($3)]) : rowType = RecordType(ANY EXPR$0, ANY EXPR$1, ANY EXPR$2, ANY EXPR$3): rowcount = 1.0, cumulative cost = {25.0 rows, 109.0 cpu, 2247.0 io, 0.0 network, 0.0 memory}, id = 159
00-02        SelectionVectorRemover : rowType = RecordType(ANY n_nationkey, ANY n_name, ANY n_regionkey, ANY n_comment): rowcount = 1.0, cumulative cost = {24.0 rows, 93.0 cpu, 2247.0 io, 0.0 network, 0.0 memory}, id = 158
00-03          Limit(fetch=[1]) : rowType = RecordType(ANY n_nationkey, ANY n_name, ANY n_regionkey, ANY n_comment): rowcount = 1.0, cumulative cost = {23.0 rows, 92.0 cpu, 2247.0 io, 0.0 network, 0.0 memory}, id = 157
00-04            Scan(table=[[dfs, tmp, text_nation]], groupscan=[EasyGroupScan [... schema=null, usedMetastore=false...
```

#### Compute table metadata and store in the Drill Metastore

Enable Drill Metastore:

```
SET `metastore.enabled` = true;
```

Specify table schema when running `ANALYZE` query:

```
ANALYZE TABLE table(dfs.tmp.`text_nation` (type=>'text', fieldDelimiter=>',', extractHeader=>true,
    schema=>'inline=(
        `n_nationkey` INT not null,
        `n_name` VARCHAR not null,
        `n_regionkey` INT not null,
        `n_comment` VARCHAR not null)'
    )) REFRESH METADATA;
+------+----------------------------------------------------------------+
|  ok  |                            summary                             |
+------+----------------------------------------------------------------+
| true | Collected / refreshed metadata for table [dfs.tmp.text_nation] |
+------+----------------------------------------------------------------+
```

#### Inspect the Metastore using INFORMATION_SCHEMA tables

Run the following query to inspect `text_nation` table schema stored in the Metastore:

```
SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_NAME='text_nation';
+-------------+-------------------+
| COLUMN_NAME |     DATA_TYPE     |
+-------------+-------------------+
| n_nationkey | INTEGER           |
| n_name      | CHARACTER VARYING |
| n_regionkey | INTEGER           |
| n_comment   | CHARACTER VARYING |
+-------------+-------------------+
```

Ensure that this schema is applied to the table:

```
SELECT typeof(n_nationkey),
       typeof(n_name),
       typeof(n_regionkey),
       typeof(n_comment)
FROM dfs.tmp.text_nation
LIMIT 1;
+--------+---------+--------+---------+
| EXPR$0 | EXPR$1  | EXPR$2 | EXPR$3  |
+--------+---------+--------+---------+
| INT    | VARCHAR | INT    | VARCHAR |
+--------+---------+--------+---------+
```

```
select sum(n_nationkey) from dfs.tmp.`text_nation`;
+--------+
| EXPR$0 |
+--------+
| 300    |
+--------+
```

Query plan contains schema from the Metastore and group scan with `usedMetastore = true`:

```
00-00    Screen : rowType = RecordType(ANY EXPR$0): rowcount = 1.0, cumulative cost = {45.1 rows, 287.1 cpu, 2247.0 io, 0.0 network, 0.0 memory}, id = 3129
00-01      Project(EXPR$0=[$0]) : rowType = RecordType(ANY EXPR$0): rowcount = 1.0, cumulative cost = {45.0 rows, 287.0 cpu, 2247.0 io, 0.0 network, 0.0 memory}, id = 3128
00-02        StreamAgg(group=[{}], EXPR$0=[SUM($0)]) : rowType = RecordType(ANY EXPR$0): rowcount = 1.0, cumulative cost = {44.0 rows, 286.0 cpu, 2247.0 io, 0.0 network, 0.0 memory}, id = 3127
00-03          Scan(table=[[dfs, tmp, text_nation]], groupscan=[EasyGroupScan ... schema=..., usedMetastore=true]]) ...
```
