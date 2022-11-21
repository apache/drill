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
