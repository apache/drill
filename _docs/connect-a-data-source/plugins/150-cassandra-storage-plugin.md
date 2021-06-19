---
title: "Cassandra Storage Plugin"
slug: "Cassandra Storage Plugin"
parent: "Connect a Data Source"
---

**Introduced in release:** 1.19

Drill's Cassandra storage plugin allows you to execute SQL queries against
Cassandra tables. This storage plugin implementation is based on
[Apache Calcite adapter for Cassandra](https://calcite.apache.org/docs/cassandra_adapter.html).
This plugin is also compatbile with Scylla DB.

### Supported optimizations and features

This storage plugin supports the following optimizations:

- Projection pushdown
- Filter pushdown (only expressions supported by Calcite adapter for Cassandra)
- Limit pushdown

Except for these optimizations, Cassandra storage plugin supports the schema
provisioning feature. For more details please refer to
[Specifying the Schema as Table Function Parameter](https://drill.apache.org/docs/plugin-configuration-basics/#specifying-the-schema-as-table-function-parameter).

### Configuration

The plugin can be registered in Apache Drill using the drill web interface by
navigating to the `storage` page. Following is the default registration
configuration.

```json
{
  "type" : "cassandra",
  "username" : null,
  "password" : null,
  "host" : "localhost",
  "port" : 9042,
  "enabled": false
}
```

### Configuration Options

| Option   | Default   | Description                                          |
| -------- | --------- | ---------------------------------------------------- |
| type     | (none)    | Set to "cassandra" to use this plugin                |
| username | null      | Cassandra username to be used by Drill               |
| password | null      | Cassandra password to be used by Drill               |
| host     | localhost | Cassandra host to be queried by Drill                |
| port     | 9042      | TCP port over which Drill will connect to Cassandra. |
