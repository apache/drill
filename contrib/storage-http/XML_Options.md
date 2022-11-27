# XML Options
Drill has a several XML configuration options to allow you to configure how Drill interprets XML files.

## DataLevel
XML data often contains a considerable amount of nesting which is not necessarily useful for data analysis. This parameter allows you to set the nesting level
  where the data actually starts.  The levels start at `1`.

## Schema Provisioning
One of the challenges of querying APIs is inconsistent data.  Drill allows you to provide a schema for individual endpoints.  You can do this in one of three ways:

1. By providing a schema inline [See: Specifying Schema as Table Function Parameter](https://drill.apache.org/docs/plugin-configuration-basics/#specifying-the-schema-as-table-function-parameter)
2. By providing a schema in the configuration for the endpoint.

Note: At the time of writing Drill's XML reader only supports provided schema with scalar data types.

## Example Configuration:
You can set either of these options on a per-endpoint basis as shown below:

```json
"xmlOptions": {
  "dataLevel": 1
}
```

Or,
```json
"xmlOptions": {
  "dataLevel": 2,
  "schema": {
    "type": "tuple_schema",
      "columns": [
        {
          "name": "custom_field",
          "type": "VARCHAR
        }
    ]
  }
}
```
