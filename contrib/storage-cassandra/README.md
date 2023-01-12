# Drill Cassandra Plugin

Drill Cassandra storage plugin allows you to perform SQL queries against Cassandra tables.
This storage plugin implementation is based on [Apache Calcite adapter for Cassandra](https://calcite.apache.org/docs/cassandra_adapter.html).

This storage plugin may be used for querying Scylla DB.

## Supported Optimizations and Features

This storage plugin supports the following optimizations:

- Project pushdown
- Filter pushdown (only expressions supported by Calcite adapter for Cassandra)
- Limit pushdown

Except for these optimizations, Cassandra storage plugin supports the schema provisioning feature.
For more details please refer to [Specifying the Schema as Table Function Parameter](https://drill.apache.org/docs/plugin-configuration-basics/#specifying-the-schema-as-table-function-parameter).

## Plugin Registration

The plugin can be registered in Apache Drill using the drill web interface by navigating to the `storage` page.
Following is the default registration configuration.

```json
{
  "type" : "cassandra",
  "host" : "localhost",
  "port" : 9042,
  "username" : null,
  "password" : null,
  "enabled": false
}
```

### User Translation
The Cassandra plugin supports user translation, which allows each user to authenticate using their own credentials instead of using system-wide credentials.  Simply set the `authMode` parameter to `USER_TRANSLATION` and use either the plain or vault credential provider for credentials.

## Developer Notes

Most of the common classes required for creating storage plugins based on Calcite adapters are placed in the 
`java-exec` module, so they can be reused in future plugin implementations.

Here is the list of the classes that may be useful:

- `VertexDrelConverterRule` with `VertexDrel` - used to hold plugin-specific part of the plan at the end of the 
  `LOGICAL` planning phase.
- `EnumerableIntermediatePrelConverterRule` with `EnumerableIntermediatePrel` - the same as above, but for the 
  `PHYSICAL` planning phase.
- `EnumerablePrel` - responsible for generating java code that will be executed to query the storage plugin data source.
- `EnumerableRecordReader` - executes java code generated in `EnumerablePrel` and transforms obtained results to Drill internal representation.
