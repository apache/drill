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
One of the challenges of querying APIs is inconsistent data.  Drill allows you to provide a schema for individual endpoints.  You can do this in one of two ways: either by 
providing a serialized TupleMetadata of the desired schema.  This is an advanced functionality and should only be used by advanced Drill users.

The schema provisioning currently supports complex types of Arrays and Maps at any nesting level.

### Example Schema Provisioning:
```json
"jsonOptions": {
  "providedSchema": [
    {
      "fieldName": "int_field",
      "fieldType": "bigint"
    }, {
      "fieldName": "jsonField",
      "fieldType": "varchar",
      "properties": {
        "json-mode":"json"
      }
    },{
      // Array field
      "fieldName": "stringField",
      "fieldType": "varchar",
      "isArray": true
    }, {
      // Map field
      "fieldName": "mapField",
      "fieldType": "map",
      "fields": [
        {
          "fieldName": "nestedField",
          "fieldType": "int"
        },{
          "fieldName": "nestedField2",
          "fieldType": "varchar"
        }
      ]
    }
  ]
}
```

### Inconsistent Schemas 
One of the major challenges of interacting with JSON data is when the schema is inconsistent.  Drill has a `UNION` data type which is marked as experimental. At the time of 
writing, the HTTP plugin does not support the `UNION`, however supplying a schema can solve a lot of those issues.

### Example Provisioning the Schema with a JSON String
```json
"jsonOptions": {
  "jsonSchema": "{\"type\":\"tuple_schema\",\"columns\":[{\"name\":\"outer_map\",\"type\":\"STRUCT<`int_field` BIGINT, `int_array` ARRAY<BIGINT>>\",\"mode\":\"REQUIRED\"}]}"
}
```


