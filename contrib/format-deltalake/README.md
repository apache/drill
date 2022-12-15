# Delta Lake format plugin

This format plugin enables Drill to query Delta Lake tables.

## Supported optimizations and features

### Project pushdown

This format plugin supports project and filter pushdown optimizations.

For the case of project pushdown, only columns specified in the query will be read, even when they are nested columns.

### Filter pushdown

For the case of filter pushdown, all expressions supported by Delta Lake API will be pushed down, so only data that
matches the filter expression will be read. Additionally, filtering logic for parquet files is enabled
to allow pruning of parquet files that do not match the filter expression.

### Querying specific table versions (snapshots)

Delta Lake has the ability to travel back in time to the specific data version.

The following ways of specifying data version are supported:

- `version` - the version number of the specific snapshot
- `timestamp` - the timestamp in milliseconds at or before which the specific snapshot was generated

Table function can be used to specify one of the above configs in the following way:

```sql
SELECT *
FROM table(dfs.tmp.testAllTypes(type => 'delta', version => 0));

SELECT *
FROM table(dfs.tmp.testAllTypes(type => 'delta', timestamp => 1636231332000));
```

## Configuration

The format plugin has the following configuration options:

- `type` - format plugin type, should be `'delta'`

### Format config example:

```json
{
  "type": "file",
  "formats": {
    "delta": {
      "type": "delta"
    }
  }
}
```
