# JSON Options and Configuration 

Drill has a collection of JSON configuration options to allow you to configure how Drill interprets JSON files.  These are set at the global level, however the HTTP plugin
allows you to configure these options individually per connection and override the Drill defaults.  The options are:

* `allowNanInf`:  Configures the connection to interpret `NaN` and `Inf` values
* `allTextMode`:  By default, Drill attempts to infer data types from JSON data. If the data is malformed, Drill may throw schema change exceptions. If your data is
  inconsistent, you can enable `allTextMode` which when true, Drill will read all JSON values as strings, rather than try to infer the data type.
* `readNumbersAsDouble`:  By default Drill will attempt to interpret integers, floating point number types and strings.  One challenge is when data is consistent, Drill may
  throw schema change exceptions. In addition to `allTextMode`, you can make Drill less sensitive by setting the `readNumbersAsDouble` to `true` which causes Drill to read all
  numeric fields in JSON data as `double` data type rather than trying to distinguish between ints and doubles.
* `enableEscapeAnyChar`:  Allows a user to escape any character with a \
* `skipMalformedRecords`:  Allows Drill to skip malformed records and recover without throwing exceptions.
* `skipMalformedDocument`:  Allows Drill to skip entire malformed documents without throwing errors.

All of these can be set by adding the `jsonOptions` to your connection configuration as shown below:

```json

"jsonOptions": {
  "allTextMode": true, 
  "readNumbersAsDouble": true
}

```

## Schema Provisioning
One of the challenges of querying APIs is inconsistent data.  Drill allows you to provide a schema for individual endpoints.  You can do this in one of three ways: 

1. By providing a schema inline [See: Specifying Schema as Table Function Parameter](https://drill.apache.org/docs/plugin-configuration-basics/#specifying-the-schema-as-table-function-parameter)
2. By providing a schema in the configuration for the endpoint.

The schema provisioning currently supports complex types of Arrays and Maps at any nesting level.

### Example Schema Provisioning:
```json
"jsonOptions": {
  "schema": {
    "columns":[
      {
        "name":"outer_map",
        "type":"ARRAY<STRUCT<`bigint_col` BIGINT, `boolean_col` BOOLEAN, `date_col` DATE, `double_col` DOUBLE, `interval_col` INTERVAL, `int_col` BIGINT, `timestamp_col` TIMESTAMP, `time_col` TIME, `varchar_col` VARCHAR>>","mode":"REPEATED"
      }, {
        "name":"field1",
        "type":"VARCHAR",
        "mode":"OPTIONAL"
      },
    ]
  }
}
```

## Dealing With Inconsistent Schemas
One of the major challenges of interacting with JSON data is when the schema is inconsistent.  Drill has a `UNION` data type which is marked as experimental. At the time of
writing, the HTTP plugin does not support the `UNION`, however supplying a schema can solve a lot of those issues.

### Json Mode
Drill offers the option of reading all JSON values as a string. While this can complicate downstream analytics, it can also be a more memory-efficient way of reading data with 
inconsistent schema.  Unfortunately, at the time of writing, JSON-mode is only available with a provided schema.  However, future work will allow this mode to be enabled for 
any JSON data.

#### Enabling JSON Mode:
You can enable JSON mode simply by adding the `drill.json-mode` property with a value of `json` to a field, as shown below:

```json
"jsonOptions": {
  "readNumbersAsDouble": true,
  "schema": {
    "type": "tuple_schema",
      "columns": [
        {
          "name": "custom_fields",
          "type": "ARRAY<STRUCT<`value` VARCHAR PROPERTIES { 'drill.json-mode' = 'json' }>>",
          "mode": "REPEATED"
      }
    ]
  }
}
```
