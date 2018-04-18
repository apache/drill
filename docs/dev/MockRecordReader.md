# MockRecordReader

Drill provides a mock record reader to generate test data in the package: `org.apache.drill.exec.store.mock`.

Mock data is available for physical plans. Here is a typical example:

```
       {
            @id:1,
            pop:"mock-scan",
            url: "http://apache.org",
            entries:[
                {records: 1000000, types: [
                  {name: "blue", type: "INT", mode: "REQUIRED"},
                  {name: "green", type: "INT", mode: "REQUIRED"}
                ]}
            ]
        },
```

The JSON fields are:

* `@id`: Standard required Physical OPerator (pop) ID.
* `pop`: Must be `"mock-scan"`
* `url`: Unused
* `entries`: A list of schemas of the files to generate.

The scanner supports parallelization based on the number of entries.

The fields for the schema are:

* `records`: The number of records to generate.
* `types`: A list of columns (the "types" name is a misnomer.)

Field schema fields include:

* `name`: Field name
* `type`: The Drill minor type as defined in `MinorType`.
* `mode`: The cardinality (mode) as defined in `DataMode`: `OPTIONAL`, `REQUIRED` or `REPEATED`.
* `width`: Optional field width (need only for variable-size types such as `VARCHAR`.)
