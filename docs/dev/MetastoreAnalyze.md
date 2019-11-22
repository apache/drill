# Metastore ANALYZE commands

Drill provides the functionality to collect, use and store table metadata into Drill Metastore.

Set `metastore.enabled` option to true to enable Metastore usage.

To collect table metadata, the following command should be used:

```
ANALYZE TABLE [table_name] [COLUMNS (col1, col2, ...)]
REFRESH METADATA [partition LEVEL]
{COMPUTE | ESTIMATE} | STATISTICS [(column1, column2, ...)]
[ SAMPLE numeric PERCENT ]
```

For the case when this command is executed for the first time, whole table metadata will be collected and stored into
 Metastore.
If analyze was already executed for the table, and table data wasn't changed, all further analyze commands wouldn't
 trigger table analyzing and message that table metadata is up to date will be returned.

# Incremental analyze

For the case when some table data was updated, Drill will try to execute incremental analyze - calculate metadata only
 for updated data and reuse required metadata from the Metastore.

Incremental analyze wouldn't be produced for the following cases:
 - list of interesting columns specified in analyze is not a subset of interesting columns from the previous analyze;
 - specified metadata level differs from the metadata level in previous analyze.

# Metadata usage

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

# Metastore related options

 - `metastore.enabled` - enables Drill Metastore usage to be able to store table metadata during `ANALYZE TABLE` commands 
execution and to be able to read table metadata during regular queries execution or when querying some `INFORMATION_SCHEMA` tables.
 - `metastore.metadata.store.depth_level` - specifies maximum level depth for collecting metadata.
 Possible values : `TABLE`, `SEGMENT`, `PARTITION`, `FILE`, `ROW_GROUP`, `ALL`.
 - `metastore.metadata.use_schema` - enables schema usage, stored to the Metastore.
 - `metastore.metadata.use_statistics` - enables statistics usage, stored in the Metastore, at the planning stage.
 - `metastore.metadata.fallback_to_file_metadata` - allows using file metadata cache for the case when required metadata is absent in the Metastore.
 - `metastore.retrieval.retry_attempts` - specifies the number of attempts for retrying query planning after detecting that query metadata is changed. 
 If the number of retries was exceeded, query will be planned without metadata information from the Metastore.
 
# Analyze operators description

Entry point for `ANALYZE` command is `MetastoreAnalyzeTableHandler` class. It creates plan which includes some
Metastore specific operators for collecting metadata.

`MetastoreAnalyzeTableHandler` uses `AnalyzeInfoProvider` for providing the information
required for building a suitable plan for collecting metadata.
Each group scan should provide corresponding `AnalyzeInfoProvider` implementation class.

Analyze command specific operators:
 - `MetadataAggBatch` - operator which adds aggregate calls for all incoming table columns to calculate required
  metadata and produces aggregations. If aggregation is performed on top of another aggregation,
  required aggregate calls for merging metadata will be added.
 - `MetadataHandlerBatch` - operator responsible for handling metadata returned by incoming aggregate operators and
  fetching required metadata form the Metastore to produce further aggregations.
 - `MetadataControllerBatch` - responsible for converting obtained metadata, fetching absent metadata from the Metastore
  and storing resulting metadata into the Metastore.

`MetastoreAnalyzeTableHandler` forms plan depending on segments count in the following form:

```
MetadataControllerRel
  ...
    MetadataHandlerRel
      MetadataAggRel(dir0, ...)
        MetadataHandlerRel
          MetadataAggRel(dir0, dir1, ...)
            MetadataHandlerRel
              MetadataAggRel(dir0, dir1, fqn, ...)
                DrillScanRel(DYNAMIC_STAR **, ANY fqn, ...)
```

For the case when `ANALYZE` uses columns for which statistics is present in parquet metadata,
`ConvertMetadataAggregateToDirectScanRule` rule will be applied to the 

```
MetadataAggRel(dir0, dir1, fqn, ...)
  DrillScanRel(DYNAMIC_STAR **, ANY fqn, ...)
```

plan part and convert it to the `DrillDirectScanRel` populated with row group metadata for the case when `ANALYZE`
was done for `ROW_GROUP` metadata level.
For the case when metadata level in `ANALYZE` is not `ROW_GROUP`, the plan above will be converted into the following plan:

```
MetadataAggRel(metadataLevel=FILE (or another non-ROW_GROUP value), createNewAggregations=false)
  DrillDirectScanRel
```

When it is converted into the physical plan, two-phase aggregation may be used for the case when incoming row
count is greater than `planner.slice_target` option value. In this case, the lowest aggregation will be hash
aggregation and it will be executed on the same minor fragments where the scan is produced. `Sort` operator will be
placed above hash aggregation. `HashToMergeExchange` operator above `Sort` will send aggregated sorted data to the
stream aggregate above.

Example of the resulting plan:

```
MetadataControllerPrel
  ...
    MetadataStreamAggPrel(PHASE_1of1)
      SortPrel
        MetadataHandlerPrel
          MetadataStreamAggPrel(PHASE_2of2)
            HashToMergeExchangePrel
              SortPrel
                MetadataHashAggPrel(PHASE_1of2)
                  ScanPrel
```

The lowest `MetadataStreamAggBatch` (or `MetadataHashAggBatch` for the case of two-phase aggregation with
`MetadataStreamAggBatch` above) creates required aggregate calls for every (or interesting only) table columns
and produces aggregations with grouping by segment columns that correspond to specific table level.
`MetadataHandlerBatch` above it populates batch with additional information about metadata type and other info.
`MetadataStreamAggBatch` above merges metadata calculated before to obtain metadata for parent metadata levels and also stores incoming data to populate it to the Metastore later.

`MetadataControllerBatch` obtains all calculated metadata, converts it to the suitable form and sends it to the Metastore.

For the case of incremental analyze, `MetastoreAnalyzeTableHandler` creates Scan with updated files only
and provides `MetadataHandlerBatch` with information about metadata which should be fetched from the Metastore, so existing actual metadata wouldn't be recalculated.
