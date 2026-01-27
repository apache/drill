# Apache Paimon format plugin

This format plugin enables Drill to query Apache Paimon tables.

Unlike regular format plugins, the Paimon table is a folder with data and metadata files, but Drill checks the presence
of the `snapshot` directory and `schema` directory to ensure that the table is a Paimon one.

Drill supports reading all formats of Paimon tables currently supported via Paimon Java API: Parquet and ORC.
No need to provide actual table format, it will be discovered automatically.

For details related to Apache Paimon table format, please refer to [official docs](https://paimon.apache.org/).

## Supported optimizations and features

### Project pushdown

This format plugin supports project pushdown optimization.

For the case of project pushdown, only columns specified in the query will be read. In conjunction with
column-oriented formats like Parquet or ORC, it allows improving reading performance significantly.

### Filter pushdown

This format plugin supports filter pushdown optimization.

For the case of filter pushdown, expressions supported by Paimon API will be pushed down, so only data that matches
the filter expression will be read.

### Limit pushdown

This format plugin supports limit pushdown optimization.

The limit is pushed down to Paimon scan planning to reduce the amount of data read.

### Querying table metadata

Apache Drill provides the ability to query table metadata exposed by Paimon.

At this point, Apache Paimon has the following metadata kinds:

* SNAPSHOTS
* SCHEMAS
* FILES
* MANIFESTS

To query specific metadata, just add the `#metadata_name` suffix to the table location, like in the following example:

```sql
SELECT *
FROM dfs.tmp.`testTable#snapshots`;
```

### Querying specific table versions (time travel)

Apache Paimon has the ability to track the table modifications and read specific version before or after modifications
or modifications itself.

This format plugin embraces this ability and provides an easy-to-use way of triggering it.

The following ways of specifying table version are supported:

- `snapshotId` - id of the specific snapshot
- `snapshotAsOfTime` - the most recent snapshot as of the given time in milliseconds

Table function can be used to specify one of the above configs in the following way:

```sql
SELECT *
FROM table(dfs.tmp.testTable(type => 'paimon', snapshotId => 1));

SELECT *
FROM table(dfs.tmp.testTable(type => 'paimon', snapshotAsOfTime => 1736345510000));
```

Note: `snapshotId` and `snapshotAsOfTime` are mutually exclusive and cannot be specified at the same time.

## Configuration

The only required configuration option is:

- `type` - format plugin type, should be `'paimon'`

Note: `snapshotId` and `snapshotAsOfTime` for time travel queries are specified at query time using the `table()` function.

### Format config example:

```json
{
  "type": "file",
  "formats": {
    "paimon": {
      "type": "paimon"
    }
  }
}
```
