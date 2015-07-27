---
title: "JSON Data Model"
parent: "Data Sources and File Formats"
---
Drill supports [JSON (JavaScript Object Notation)](http://www.json.org/), a self-describing data format. The data itself implies its schema and has the following characteristics:

* Language-independent
* Textual format
* Loosely defined, weak data typing

Semi-structured JSON data often consists of complex, nested elements having schema-less fields that differ type-wise from row to row. The data can constantly evolve. Applications typically add and remove fields frequently to meet business requirements.

Using Drill you can natively query dynamic JSON data sets using SQL. Drill treats a JSON object as a SQL record. One object equals one row in a Drill table.

You can also [query compressed .gz files]({{ site.baseurl }}/docs/querying-plain-text-files/#querying-compressed-files) having JSON as well as uncompressed .json files.

In addition to the examples presented later in this section, see ["How to Analyze Highly Dynamic Datasets with Apache Drill"](https://www.mapr.com/blog/how-analyze-highly-dynamic-datasets-apache-drill) for information about how to analyze a JSON data set.

## Data Type Mapping
JSON data consists of the following types:

* Array: ordered values, separated by commas, enclosed in square brackets
* Boolean: true or false
* Number: double-precision floating point number, including exponential numbers. No octal, hexadecimal, NaN, or Infinity
* null: empty value
* Object: unordered key/value collection enclosed in curly braces
* String: Unicode enclosed in double quotation marks
* Value: a string, number, true, false, null
* Whitespace: used between tokens

The following table shows SQL-JSON data type mapping:

| SQL Type | JSON Type | Description                                                                                   |
|----------|-----------|-----------------------------------------------------------------------------------------------|
| BOOLEAN  | Boolean   | True or false                                                                                 |
| BIGINT   | Numeric   | Number having no decimal point in JSON, 8-byte signed integer in Drill                        |
| DOUBLE   | Numeric   | Number having a decimal point in JSON, 8-byte double precision floating point number in Drill |
| VARCHAR  | String    | Character string of variable length                                                           |

By default, Drill does not support JSON lists of different types. For example, JSON does not enforce types or distinguish between integers and floating point values. When reading numerical values from a JSON file, Drill distinguishes integers from floating point numbers by the presence or lack of a decimal point. If some numbers in a JSON map or array appear with and without a decimal point, such as 0 and 0.0, Drill throws a schema change error. You use the following options to read JSON lists of different types:

* `store.json.read_numbers_as_double`  
  Reads numbers from JSON files with or without a decimal point as DOUBLE. You need to cast numbers from DOUBLE to other numerical types only if you cannot use the numbers as DOUBLE.
* `store.json.all_text_mode`  
  Reads all data from JSON files as VARCHAR. You need to cast numbers from VARCHAR to numerical data types, such as DOUBLE or INTEGER.

The default setting of `store.json.all_text_mode` and `store.json.read_numbers_as_double` options is false. Using either option prevents schema errors, but using `store.json.read_numbers_as_double` has an advantage over `store.json.all_text_mode`. Using `store.json.read_numbers_as_double` typically involves less explicit casting than using `store.json.all_text_mode` because you can often use the numerical data as is-\-DOUBLE.

### Handling Type Differences
Set the `store.json.read_numbers_as_double` property to true.

    ALTER SYSTEM SET `store.json.read_numbers_as_double` = true;

When you set this option, Drill reads all numbers from the JSON files as DOUBLE. After reading the data, use a SELECT statement in Drill to cast data as follows:

* Cast JSON values to [SQL types]({{ site.baseurl }}/docs/json-data-model/#data-type-mapping), such as BIGINT, FLOAT, and INTEGER.
* Cast JSON strings to [Drill Date/Time Data Type Formats]({{ site.baseurl }}/docs/date-time-and-timestamp).

[“Query Complex Data”]({{ site.baseurl }}/docs/querying-complex-data-introduction) show how to use [composite types]({{site.baseurl}}/docs/supported-data-types/#composite-types) to access nested arrays.

Drill uses these types internally for reading complex and nested data structures from data sources such as JSON.

## Reading JSON
To read JSON data using Drill, use a [file system storage plugin]({{ site.baseurl }}/docs/file-system-storage-plugin/) that defines the JSON format. You can use the `dfs` storage plugin, which includes the definition.

JSON data is often complex. Data can be deeply nested and semi-structured. but you can use [workarounds ]({{ site.baseurl }}/docs/json-data-model/#limitations-and-workarounds) covered later.

Drill reads tuples defined in single objects, having no comma between objects. A JSON object is an unordered set of name/value pairs. Curly braces delimit objects in the JSON file:

    { name: "Apples", desc: "Delicious" }
    { name: "Oranges", desc: "Florida Navel" }

To read and [analyze complex JSON]({{ site.baseurl }}/docs/json-data-model#analyzing-json) files, use the FLATTEN and KVGEN functions.

## Writing JSON
You can write data from Drill to a JSON file. The following setup is required:

* In the storage plugin definition, include a writable (mutable) workspace. For example:

      {
      . . .
        "workspaces": {
      . . .
          "myjsonstore": {
            "location": "/tmp",
            "writable": true,
          }
      . . .

* Set the output format to JSON. For example:

        ALTER SESSION SET `store.format`='json';

* Use the path to the workspace location in a CTAS command. for example:

        USE myplugin.myworkspace;
        CREATE TABLE my_json AS
        SELECT my column from dfs.`<path_file_name>`;

Drill performs the following actions, as shown in the complete [CTAS command example]({{ site.baseurl }}/docs/create-table-as-ctas/):

* Creates a directory using table name.
* Writes the JSON data to the directory in the workspace location.

## Analyzing JSON

Generally, you query JSON files using the following syntax, which includes a table alias. The alias is sometimes required for querying complex data. Because of the ambiguity between y.z where y could be a column or a table,
Drill currently explicitly requires a table prefix for referencing a field
inside another field (t.y.z).  This isn't required in the case y, y[z] or
y[z].x because these references are not ambiguous. Observe the following guidelines:

* Use dot notation to drill down into a JSON map.

        SELECT t.level1.level2. . . . leveln FROM <storage plugin location>`myfile.json` t

* Use square brackets, array-style notation to drill down into a JSON array.

        SELECT t.level1.level2[n][2] FROM <storage plugin location>`myfile.json` t;

  The first index position of an array is 0.

* Do not use a map, array or repeated scalar type in GROUP BY, ORDER BY or in a comparison operator.

Drill returns null when a document does not have the specified map or level.

Use the following techniques to query complex, nested JSON:

* Flatten nested data
* Generate key/value pairs for loosely structured data

## Example: Flatten and Generate Key Values for Complex JSON

This example uses the following data that represents unit sales of tickets to events that were sold over a period of several days in December:

### ticket_sales.json Contents

    {
      "type": "ticket",
      "venue": 123455,
      "sales": {
        "12-10": 532806,
        "12-11": 112889,
        "12-19": 898999,
        "12-21": 10875
      }
    }
    {
      "type": "ticket",
      "venue": 123456,
      "sales": {
        "12-10": 87350,
        "12-15": 972880,
        "12-19": 49999,
        "12-21": 857475
      }
    }

Take a look at the data in Drill:

    +---------+---------+---------------------------------------------------------------+
    |  type   |  venue  |                             sales                             |
    +---------+---------+---------------------------------------------------------------+
    | ticket  | 123455  | {"12-10":532806,"12-11":112889,"12-19":898999,"12-21":10875}  |
    | ticket  | 123456  | {"12-10":87350,"12-19":49999,"12-21":857475,"12-15":972880}   |
    +---------+---------+---------------------------------------------------------------+
    2 rows selected (1.343 seconds)


### Generate Key/Value Pairs
Continuing with the data from [previous example]({{site.baseurl}}/docs/json-data-model/#example:-flatten-and-generate-key-values-for-complex-json), use the KVGEN (Key Value Generator) function to generate key/value pairs from complex data. Generating key/value pairs is often helpful when working with data that contains arbitrary maps consisting of dynamic and unknown element names, such as the ticket sales data in this example. For example purposes, take a look at how kvgen breaks the sales data into keys and values representing the key dates and number of tickets sold:

    SELECT KVGEN(tkt.sales) AS `key dates:tickets sold` FROM dfs.`/Users/drilluser/ticket_sales.json` tkt;
    +---------------------------------------------------------------------------------------------------------------------------------------+
    |                                                        key dates:tickets sold                                                         |
    +---------------------------------------------------------------------------------------------------------------------------------------+
    | [{"key":"12-10","value":"532806"},{"key":"12-11","value":"112889"},{"key":"12-19","value":"898999"},{"key":"12-21","value":"10875"}] |
    | [{"key":"12-10","value":"87350"},{"key":"12-19","value":"49999"},{"key":"12-21","value":"857475"},{"key":"12-15","value":"972880"}] |
    +---------------------------------------------------------------------------------------------------------------------------------------+
    2 rows selected (0.106 seconds)

KVGEN allows queries against maps where the keys themselves represent data rather than a schema, as shown in the next example.

### Flatten JSON Data

FLATTEN breaks the list of key-value pairs into separate rows on which you can apply analytic functions. FLATTEN takes a JSON array, such as the output from kvgen(sales), as an argument. Using the all (*) wildcard as the argument is not supported and returns an error. The following example continues using data from the [previous example]({{site.baseurl}}/docs/json-data-model/#example:-flatten-and-generate-key-values-for-complex-json):

    SELECT FLATTEN(kvgen(sales)) Sales
    FROM dfs.`/Users/drilluser/drill/ticket_sales.json`;

    +--------------------------------+
    |           Sales                |
    +--------------------------------+
    | {"key":"12-10","value":532806} |
    | {"key":"12-11","value":112889} |
    | {"key":"12-19","value":898999} |
    | {"key":"12-21","value":10875}  |
    | {"key":"12-10","value":87350}  |
    | {"key":"12-19","value":49999}  |
    | {"key":"12-21","value":857475} |
    | {"key":"12-15","value":972880} |
    +--------------------------------+
    8 rows selected (0.171 seconds)

### Example: Aggregate Loosely Structured Data
Use flatten and kvgen together to aggregate the data from the [previous example]({{site.baseurl}}/docs/json-data-model/#example:-flatten-and-generate-key-values-for-complex-json). Make sure all text mode is set to false to sum numbers. Drill returns an error if you attempt to sum data in all text mode.

    ALTER SYSTEM SET `store.json.all_text_mode` = false;

Sum the ticket sales by combining the `SUM`, `FLATTEN`, and `KVGEN` functions in a single query.

    SELECT SUM(tkt.tot_sales.`value`) AS TicketSold FROM (SELECT flatten(kvgen(sales)) tot_sales FROM dfs.`/Users/drilluser/ticket_sales.json`) tkt;

    +--------------+
    | TicketsSold  |
    +--------------+
    | 3523273.0    |
    +--------------+
    1 row selected (0.244 seconds)

### Example: Aggregate and Sort Data

Sum and group the ticket sales by date and sort in ascending order of total tickets sold.

    SELECT `right`(tkt.tot_sales.key,2) `December Date`,
    SUM(tkt.tot_sales.`value`) AS TotalSales
    FROM (SELECT FLATTEN(kvgen(sales)) tot_sales
    FROM dfs.`/Users/drilluser/ticket_sales.json`) tkt
    GROUP BY `right`(tkt.tot_sales.key,2)
    ORDER BY TotalSales;

    +----------------+-------------+
    | December Date  | TotalSales  |
    +----------------+-------------+
    | 11             | 112889.0    |
    | 10             | 620156.0    |
    | 21             | 868350.0    |
    | 19             | 948998.0    |
    | 15             | 972880.0    |
    +----------------+-------------+
    5 rows selected (0.252 seconds)

### Example: Access a Map Field in an Array
To access a map field in an array, use dot notation to drill down through the hierarchy of the JSON data to the field. Examples are based on the following [City Lots San Francisco in .json](https://github.com/zemirco/sf-city-lots-json), modified slightly as described in the empty array workaround in ["Limitations and Workarounds."]({{ site.baseurl }}/docs/json-data-model#empty-array)

    {
      "type": "FeatureCollection",
      "features": [
      {
        "type": "Feature",
        "properties":
        {
          "MAPBLKLOT": "0001001",
          "BLKLOT": "0001001",
          "BLOCK_NUM": "0001",
          "LOT_NUM": "001",
          "FROM_ST": "0",
          "TO_ST": "0",
          "STREET": "UNKNOWN",
          "ST_TYPE": null,
          "ODD_EVEN": "E" },
          "geometry":
        {
            "type": "Polygon",
            "coordinates":
            [ [
            [ -122.422003528252475, 37.808480096967251, 0.0 ],
            [ -122.422076013325281, 37.808835019815085, 0.0 ],
            [ -122.421102174348633, 37.808803534992904, 0.0 ],
            [ -122.421062569067274, 37.808601056818148, 0.0 ],
            [ -122.422003528252475, 37.808480096967251, 0.0 ]
            ] ]
        }
      },
    . . .

This example shows how to drill down using array notation plus dot notation in features[0].properties.MAPBLKLOT to get the MAPBLKLOT property value in the San Francisco city lots data:

    SELECT features[0].properties.MAPBLKLOT, FROM dfs.`/Users/drilluser/citylots.json`;

    +------------+
    |   EXPR$0   |
    +------------+
    | 0001001    |
    +------------+
    1 row selected (0.163 seconds)

To access the second geometry coordinate of the first city lot in the San Francisco city lots, use array indexing notation for the coordinates as well as the features:

    SELECT features[0].geometry.coordinates[0][1]
    FROM dfs.`/Users/drilluser/citylots.json`;
    +-------------------+
    |      EXPR$0       |
    +-------------------+
    | 37.80848009696725 |
    +-------------------+
    1 row selected (0.19 seconds)

More examples of drilling down into an array are shown in ["Selecting Nested Data for a Column"]({{ site.baseurl }}/docs/selecting-nested-data-for-a-column).

### Example: Flatten an Array of Maps using a Subquery
By flattening the following JSON file, which contains an array of maps, you can evaluate the records of the flattened data.

    {"name":"classic","fillings":[ {"name":"sugar","cal":500} , {"name":"flour","cal":300} ] }

    SELECT flat.fill FROM (SELECT FLATTEN(t.fillings) AS fill FROM dfs.flatten.`test.json` t) flat WHERE flat.fill.cal  > 300;

    +----------------------------+
    |           fill             |
    +----------------------------+
    | {"name":"sugar","cal":500} |
    +----------------------------+
    1 row selected (0.421 seconds)

Use a table alias for column fields and functions when working with complex data sets. Currently, you must use a subquery when operating on a flattened column. Eliminating the subquery and table alias in the WHERE clause, for example `flat.fillings[0].cal > 300`, does not evaluate all records of the flattened data against the predicate and produces the wrong results.

### Example: Access Map Fields in a Map
This example uses a WHERE clause to drill down to a third level of the following JSON hierarchy to get the max_hdl greater than 160:

    {
      "SOURCE": "Allegheny County",
      "TIMESTAMP": 1366369334989,
      "birth": {
        "id": 35731300,
        "firstname": "Jane",
        "lastname": "Doe",
        "weight": "CATEGORY_1",
        "bearer": {
          "father": "John Doe",
          "ss": "208-55-5983",
          "max_ldl": 180,
          "max_hdl": 200
        }
      }
    }
    {
      "SOURCE": "Marin County",
      "TIMESTAMP": 1366369334,
        "birth": {
          "id": 35731309,
          "firstname": "Somporn",
          "lastname": "Thongnopneua",
          "weight": "CATEGORY_2",
          "bearer": {
            "father": "Jeiranan Thongnopneua",
            "ss": "208-25-2223",
            "max_ldl": 110,
            "max_hdl": 150
        }
      }
    }

Use dot notation, for example `t.birth.lastname` and `t.birth.bearer.max_hdl` to drill down to the nested level:

    SELECT t.birth.lastname AS Name, t.birth.weight AS Weight
    FROM dfs.`Users/drilluser/vitalstat.json` t
    WHERE t.birth.bearer.max_hdl < 160;

    +----------------+------------+
    |    Name        |   Weight   |
    +----------------+------------+
    | Thongneoupeanu | CATEGORY_2 |
    +----------------+------------+
    1 row selected (0.142 seconds)

## Limitations and Workarounds
In most cases, you can use a workaround, presented in the following sections, to overcome the following limitations:

* [Array at the root level]({{site.baseurl}}/docs/json-data-model/#array-at-the-root-level)
* [Complex nested data]({{site.baseurl}}/docs/json-data-model/#complex-nested-data)
* [Empty array]({{site.baseurl}}/docs/json-data-model/#empty-array)
* [Lengthy JSON objects]({{site.baseurl}}/docs/json-data-model/#lengthy-json-objects)
* [Complex JSON objects]({{site.baseurl}}/docs/json-data-model/#complex-json-objects)
* [Nested column names]({{site.baseurl}}/docs/json-data-model/#nested-column-names)
* [Schema changes]({{site.baseurl}}/docs/json-data-model/#schema-changes)
* [Selecting all in a JSON directory query]({{site.baseurl}}/docs/json-data-model/#selecting-all-in-a-json-directory-query)

### Array at the root level
Drill cannot read an array at the root level, outside an object.

Workaround: Remove square brackets at the root of the object, as shown in the following example.

![drill query flow]({{ site.baseurl }}/docs/img/datasources-json-bracket.png)

### Complex nested data
Drill cannot read some complex nested arrays unless you use a table alias.

Workaround: To query n-level nested data, use the table alias to remove ambiguity; otherwise, column names such as user_info are parsed as table names by the SQL parser. The alias is not needed for data that is not nested, as shown in the following example:

    {"dev_id": 0,
      "date":"07/26/2013",
      "time":"04:56:59",
      "user_info":
        {"user_id":28,
         "device":"A306",
         "state":"mt"
        },
        "marketing_info":
          {"promo_id":4,
           "keywords":  
            ["stay","to","think","watch","glasses",
             "joining","might","pay","in","your","buy"]
          },
          "dev_info":
            {"prod_id":[16],"purch_flag":"false"
            }
    }
    . . .

    SELECT dev_id, `date`, `time`, t.user_info.user_id, t.user_info.device, t.dev_info.prod_id
    FROM dfs.`/Users/mypath/example.json` t;

### Empty array
Drill cannot read an empty array, shown in the following example, and attempting to do so causes an error.

        { "a":[] }

Workaround: Remove empty arrays.

For example, you cannot query the [City Lots San Francisco in .json](https://github.com/zemirco/sf-city-lots-json) data unless you make the following modification.

![drill query flow]({{ site.baseurl }}/docs/img/json-workaround.png)

After removing the extraneous square brackets in the coordinates array, you can drill down to query all the data for the lots.

### Lengthy JSON objects
Currently, Drill cannot manage lengthy JSON objects, such as a gigabit JSON file. Finding the beginning and end of records can be time consuming and require scanning the whole file.

Workaround: Use a tool to split the JSON file into smaller chunks of 64-128MB or 64-256MB initially until you know the total data size and node configuration. Keep the JSON objects intact in each file. A distributed file system, such as MapR-FS, is recommended over trying to manage file partitions.

### Complex JSON objects
Complex arrays and maps can be difficult or impossible to query.

Workaround: Separate lengthy objects into objects delimited by curly braces using the following functions:

* [FLATTEN]({{ site.baseurl }}/docs/json-data-model#flatten-json-data) separates a set of nested JSON objects into individual rows in a DRILL table.

* [KVGEN]({{ site.baseurl }}/docs/kvgen/) separates objects having more elements than optimal for querying.


### Nested Column Names

You cannot use reserved words for nested column names because Drill returns null if you enclose n-level nested column names in back ticks. The previous example encloses the date and time column names in back ticks because the names are reserved words. The enclosure of column names in back ticks works because the date and time columns belong to the first level of the JSON object.

For example, the following object contains the reserved word key, which you need to rename to `_key` or something other than non-reserved word:

    {
      "type": "ticket",
      "channel": 123455,
      "_month": 12,
      "_day": [ 15, 25, 28, 31 ],
      "sales": {
        "NY": 532806,
        "PA": 112889,
        "TX": 898999,
        "UT": 10875
        "key": [ 78946, 39107, 76311 ]
      }
    }

### Schema changes
Drill cannot read JSON files containing changes in the schema. For example, attempting to query an object having array elements of different data types cause an error:

![drill query flow]({{ site.baseurl }}/docs/img/data-sources-schemachg.png)

Drill interprets numbers that do not have a decimal point as BigInt values. In this example, Drill recognizes the first two coordinates as doubles and the third coordinate as a BigInt, which causes an error.

Workaround: Set the `store.json.read_numbers_as_double` property, described earlier, to true.

    ALTER SYSTEM SET `store.json.read_numbers_as_double` = true;

### Selecting all in a JSON directory query
Drill currently returns only fields common to all the files in a [directory query]({{ site.baseurl }}/docs/querying-directories) that selects all (SELECT *) JSON files.

Workaround: Query each file individually.
