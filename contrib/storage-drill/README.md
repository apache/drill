# Apache Drill storage plugin

This storage plugin allows Drill to query other Drill clusters.
Unlike the JDBC driver, this plugin doesn't produce extra conversions of input data and transfers it
as is to the required operators. But similar to JDBC, it fetches data batches only when it is ready
to process it to avoid memory issues.

## Supported optimizations and features

Drill storage plugin supports push-down of all operators it has.
It determines which parts of the query plan could be pushed down and converts them to SQL queries
submitted to the underlying Drill cluster.

## Configuration

Drill storage plugin has the following configuration properties:

- `type` - storage plugin type, should be `'drill'`
- `connection` - JDBC connection string to connect to underlying Drill cluster. Please refer to
  [Configuration](https://drill.apache.org/docs/using-the-jdbc-driver/#using-the-jdbc-url-for-a-random-drillbit-connection) page for more details
- `properties` - Connection properties. Please refer to [Configuration](https://drill.apache.org/docs/using-the-jdbc-driver/#using-the-jdbc-url-for-a-random-drillbit-connection) page for more details
- `credentialsProvider` - credentials provider

### Storage config example:

```json
{
  "storage":{
    "drill" : {
      "type":"drill",
      "connection":"jdbc:drill:drillbit=localhost:31010",
      "enabled": false
    }
  }
}
```
