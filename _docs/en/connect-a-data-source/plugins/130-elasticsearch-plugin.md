---
title: "ElasticSearch Storage Plugin"
slug: "ElasticSearch Storage Plugin"
parent: "Connect a Data Source"
---

**Introduced in release:** 1.19

Drill's ElasticSearch storage plugin allows you to perform SQL queries against ElasticSearch indices.
This storage plugin implementation is based on [Apache Calcite adapter for ElasticSearch](https://calcite.apache.org/docs/elasticsearch_adapter.html).

For more details about supported versions please refer to [Supported versions](https://calcite.apache.org/docs/elasticsearch_adapter.html#supported-versions) page.

### Supported optimizations and features

This storage plugin supports the following optimizations:

- Project pushdown
- Filter pushdown (only expressions supported by Calcite adapter for ElasticSearch. Filter with unsupported expressions 
  wouldn't be pushed to ElasticSearch but will be produced by Drill)
- Limit pushdown
- Aggregation pushdown
- Sort pushdown

Besides these optimizations, ElasticSearch storage plugin supports the schema provisioning feature.
For more details please refer to [Specifying the Schema as Table Function Parameter](https://drill.apache.org/docs/plugin-configuration-basics/#specifying-the-schema-as-table-function-parameter).

### Configuration

The plugin can be registered in Apache Drill using the drill web interface by navigating to the `storage` page.
Following is the default registration configuration.

```json
{
  "type": "elastic",
  "hosts": [
    "http://localhost:9200"
  ],
  "username": null,
  "password": null,
  "enabled": false
}
```

### Configuration Options

|----------|-----------------------|----------------------------------------------------|
| Option   | Default               | Description                                        |
|----------|-----------------------|----------------------------------------------------|
| type     | (none)                | Set to "elastic" to use this plugin                |
| hosts    | http://localhost:9200 | List of ElasticSearch hosts to be queried by Drill |
| username | null                  | ElasticSearch username to be used by Drill         |
| password | null                  | ElasticSearch password to be used by Drill         |
|----------|-----------------------|----------------------------------------------------|

