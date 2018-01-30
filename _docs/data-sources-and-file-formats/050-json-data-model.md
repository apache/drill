---
title: "JSON Data Model"
date: 2018-01-30 05:41:07 UTC
parent: "Data Sources and File Formats"
---
Drill supports [JSON (JavaScript Object Notation)](http://www.json.org/), a self-describing data format. The data itself implies its schema and has the following characteristics:

* Language-independent
* Textual format
* Loosely defined, weak data typing

Semi-structured JSON data often consists of complex, nested elements having schema-less fields that differ type-wise from row to row. The data can constantly evolve. Applications typically add and remove fields frequently to meet business requirements.

Using Drill you can natively query dynamic JSON data sets using SQL. Drill treats a JSON object as a SQL record. One object equals one row in a Drill table.

You can also [query compressed .gz JSON files]({{ site.baseurl }}/docs/querying-plain-text-files/#querying-compressed-files).

In addition to the examples presented later in this section, see ["How to Analyze Highly Dynamic Datasets with Apache Drill"](https://www.mapr.com/blog/how-analyze-highly-dynamic-datasets-apache-drill) for information about how to analyze a JSON data set.

## Data Type Mapping
JSON data consists of the following types:

* Array: ordered values, separated by commas, enclosed in square brackets
* Boolean: true or false
* null: empty value
* Object: unordered key/value collection enclosed in curly braces
* String: Unicode enclosed in double quotation marks
* Value: a string, number, true, false, null
* Whitespace: used between tokens 
* Number: double-precision floating point number, including exponential numbers, NaN, and Infinity. Octal and hexadecimal are not supported.  

Drill 1.13 and later supports NaN (Not-a-Number) and Infinity as numeric values. This support introduces the following session options, which are set to “true” by default:  

       store.json.writer.allow_nan_inf
       store.json.reader.allow_nan_inf  

Drill writes NaN and Infinity values as literals to a JSON file when `store.json.writer.allow_nan_inf` is set to true. When set to false, Drill writes NaN and Infinity values as string values to a JSON file. If a query selects NaN or Infinity values in a JSON file, and `store.json.reader.allow_nan_inf` is set to false, Drill returns an error message. You can use the [SET]({{site.baseurl}}/docs/set/) command to change the options.
  

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

    ALTER SESSION SET `store.json.read_numbers_as_double` = true;

When you set this option, Drill reads all numbers from the JSON files as DOUBLE. After reading the data, use a SELECT statement in Drill to cast data as follows:

* Cast JSON values to [SQL types]({{ site.baseurl }}/docs/json-data-model/#data-type-mapping), such as BIGINT, FLOAT, and INTEGER.
* Cast JSON strings to [Drill Date/Time Data Type Formats]({{ site.baseurl }}/docs/date-time-and-timestamp).

[“Query Complex Data”]({{ site.baseurl }}/docs/querying-complex-data-introduction) show how to use [composite types]({{site.baseurl}}/docs/supported-data-types/#composite-types) to access nested arrays.

Drill uses these types internally for reading complex and nested data structures from data sources such as JSON.

### Experimental Feature: Heterogeneous types
The Union type allows storing different types in the same field. This new feature is still considered experimental, and must be explicitly enabled by setting the `exec.enabel_union_type` option to true.

    ALTER SESSION SET `exec.enable_union_type` = true;
    
 With this feature enabled, JSON data with changing types, which previously could not be queried by drill, are now queryable.
 
A field with a Union type can be used inside of functions. Drill will automatically handle evaluation of the function appropriately for each type. If the data requires special handling for the different types, you can do this with case statement, leveraging the new `type` functions:

```
select 1 + case when is_list(a) then a[0] else a end from table;
```

In this example, the column a contains both scalar and list types, so the case where it is a list is handled by using the first element of the array.

## Reading JSON
To read JSON data using Drill, use a [file system storage plugin]({{ site.baseurl }}/docs/file-system-storage-plugin/) that defines the JSON format. You can use the `dfs` storage plugin, which includes the definition.

JSON data is often complex. Data can be deeply nested and semi-structured. but you can use [workarounds ]({{ site.baseurl }}/docs/json-data-model/#limitations-and-workarounds) covered later.

Drill reads tuples defined in single objects, having no comma between objects. A JSON object is an unordered set of name/value pairs. Curly braces delimit objects in the JSON file:

    { name: "Apples", desc: "Delicious" }
    { name: "Oranges", desc: "Florida Navel" }

To read and [analyze complex JSON]({{ site.baseurl }}/docs/json-data-model/#analyzing-json) files, use the FLATTEN and KVGEN functions. For example, you need to flatten the data to read all the names in this JSON file:

     {"Fruits: [{"name":"Apples", "quantity":115},
                {"name":"Oranges","quantity":199},
                {"name":"Peaches", "quantity":116}
               ]
     }

To flatten the data and read the names, use a subquery that flattens the complex nesting. Use table aliases, t and flatdata, to resolve ambiguities.

     SELECT t.flatdata.name FROM (select flatten(Fruits) AS flatdata FROM dfs.`/Users/path/fruits.json`) t;

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
Drill requires a table prefix for referencing a field
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

    ALTER SESSION SET `store.json.all_text_mode` = false;

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
To access a map field in an array, use dot notation to drill down through the hierarchy of the JSON data to the field. Examples are based on the following [City Lots San Francisco in .json](https://github.com/zemirco/sf-city-lots-json).

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

More examples of workarounds for drilling down into the city lots data are presented in the following sections:

* [Commas between records]({{site.baseurl}}/docs/json-data-model/#commas-between-records)
* [Irregular data]({{site.baseurl}}/docs/json-data-model/#irregular-data)
* [Varying types]({{site.baseurl}}/docs/json-data-model/#varying-types)

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

* [Complex nested data]({{site.baseurl}}/docs/json-data-model/#complex-nested-da[ta)
* [Commas between records]({{site.baseurl}}/docs/json-data-model/#commas-between-records)
* [Irregular data]({{site.baseurl}}/docs/json-data-model/#irregular-data)
* [Varying types]({{site.baseurl}}/docs/json-data-model/#varying-types)
* [Misusing Dot Notation]({{site.baseurl}}/docs/json-data-model/#misusing-dot-notation)
* [Lengthy JSON objects]({{site.baseurl}}/docs/json-data-model/#lengthy-json-objects)
* [Complex JSON objects]({{site.baseurl}}/docs/json-data-model/#complex-json-objects)
* [Schema changes]({{site.baseurl}}/docs/json-data-model/#schema-changes)
* [Selecting all in a JSON directory query]({{site.baseurl}}/docs/json-data-model/#selecting-all-in-a-json-directory-query)

### Complex nested data
Drill cannot read some complex nested arrays unless you use a table alias.

Workaround: To query n-level nested data, use the table alias to remove ambiguity; otherwise, column names such as user_info are parsed as table names by the SQL parser. The alias is not needed for data, such as dev_id, date, and time, that are not nested:

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

``SELECT dev_id, `date`, `time`, t.user_info.user_id, t.user_info.device, t.dev_info.prod_id
FROM dfs.`/Users/mypath/example.json` t;``

### Commas between records
Continuing the example ["Accessing a Map Field in an Array"]({{site.baseurl}}/docs/json-data-model/#example-access-a-map-field-in-an-array), Drill cannot find data in multiple records separated by commas.

Workaround: Delete commas between records. 

After deleting the commas, the following query works:

    SELECT 
      lots.geometry.coordinates[0][0][0] longitude,
      lots.geometry.coordinates[0][0][1] latitude,
      lots.geometry.coordinates[0][0][2] altitude 
    FROM dfs.`/Users/drilluser/citylots.json` lots LIMIT 1;

    +-----------------------+---------------------+-----------+
    |       longitude       |      latitude       | altitude  |
    +-----------------------+---------------------+-----------+
    | -122.422003528252475  | 37.808480096967251  | 0.0       |
    +-----------------------+---------------------+-----------+
    1 row selected (0.618 seconds)

### Irregular data
Data that lacks uniformity causes a problem. Continuing the example ["Accessing a Map Field in an Array"]({{site.baseurl}}/docs/json-data-model/#example-access-a-map-field-in-an-array), Drill cannot handle the intermixed Polygon shapes and
MultiPolygon shapes.

Workaround: None, per se, but if you avoid querying the multi-polygon lines (120 of them), Drill works fine on the entire remainder. For example:

    WITH tbl AS (
    SELECT
      CAST(lots.geometry.coordinates[0][0][0] AS FLOAT) longitude, 
      CAST(lots.geometry.coordinates[0][0][1] AS FLOAT) latitude, 
      CAST(lots.geometry.coordinates[0][0][2] AS FLOAT) altitude 
    FROM dfs./Users/drilluser/uniform.json` lots) 
    SELECT 
      AVG(longitude), 
      AVG(latitude), 
      MAX(altitude) 
    FROM tbl;

    +---------------------+--------------------+---------+
    |       EXPR$0        |       EXPR$1       | EXPR$2  |
    +---------------------+--------------------+---------+
    | -122.4379846573301  | 37.75844260679518  | 0.0     |
    +---------------------+--------------------+---------+
    1 row selected (6.64 seconds)
    
Another option is to use the experimental union type.

### Varying types
Any attempt to query a list that has
varying types fails. Continuing the example ["Accessing a Map Field in an Array"]({{site.baseurl}}/docs/json-data-model/#example-access-a-map-field-in-an-array), the city lots file has a list of lists of
coordinates for shapes of type Polygon.  For shapes of MultiPolygon, this file has lists of lists
of coordinates. Even a query that tries to filter away the
MultiPolygons will fail.

Workaround: Use union type (experimental).

### Misusing Dot Notation
Drill accesses an object when you use dot notation in the SELECT statement only when the dot is *not* the first dot in the expression. Drill attempts to access the table that appears after the first dot. For example,  records in `some-file` have a geometry field that Drill successfully accesses given this query:

``select geometry from  dfs.`some-file.json`;``

The following query, however, causes an error because there is no table named geometry.

``select geometry.x from dfs.`some-file.json`;``

Workaround: Use a table alias. For example:

``select tbl.geometry.x from dfs.`some-file.json` tbl;``

### Lengthy JSON objects
Currently, Drill cannot manage lengthy JSON objects, such as a gigabit JSON file. Finding the beginning and end of records can be time consuming and require scanning the whole file.

Workaround: Use a tool to split the JSON file into smaller chunks of 64-128MB or 64-256MB initially until you know the total data size and node configuration. Keep the JSON objects intact in each file. A distributed file system, such as HDFS, is recommended over trying to manage file partitions.

### Complex JSON objects
Complex arrays and maps can be difficult or impossible to query.

Workaround: Separate lengthy objects into objects delimited by curly braces using the following functions:

* [FLATTEN]({{ site.baseurl }}/docs/json-data-model/#flatten-json-data) separates a set of nested JSON objects into individual rows in a DRILL table.

* [KVGEN]({{ site.baseurl }}/docs/kvgen/) separates objects having more elements than optimal for querying.


### Schema changes
Drill cannot read JSON files containing changes in the schema. For example, attempting to query an object having array elements of different data types cause an error:

![drill query flow]({{ site.baseurl }}/docs/img/data-sources-schemachg.png)

Drill interprets numbers that do not have a decimal point as BigInt values. In this example, Drill recognizes the first two coordinates as doubles and the third coordinate as a BigInt, which causes an error.

Workaround: Set the `store.json.read_numbers_as_double` property, described earlier, to true.

    ALTER SESSION SET `store.json.read_numbers_as_double` = true;

### Selecting all in a JSON directory query
Drill currently returns only fields common to all the files in a [directory query]({{ site.baseurl }}/docs/querying-directories) that selects all (SELECT *) JSON files.

Workaround: Query each file individually. Another option is to use the union type (experimental).
