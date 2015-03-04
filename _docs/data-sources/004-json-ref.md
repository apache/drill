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

Drill 0.8 and higher can [query compressed .gz files](/docs/drill-default-input-format#querying-compressed-json) having JSON as well as uncompressed .json files. 

In addition to the examples presented later in this section, see "How to Analyze Highly Dynamic Datasets with Apache Drill" (https://www.mapr.com/blog/how-analyze-highly-dynamic-datasets-apache-drill) for information about how to analyze a JSON data set.

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

JSON data consists of the following types: 

<table>
  <tr>
    <th>SQL Type</th>
    <th>JSON Type</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>BOOLEAN</td>
    <td>Boolean</td>
    <td>True or false</td>
  </tr>
  <tr>
    <td>BIGINT</td>
    <td>Numeric</td>
    <td>Number having no decimal point in JSON, 8-byte signed integer in Drill</td>
  </tr>
   <tr>
    <td>DOUBLE</td>
    <td>Numeric</td>
    <td>Number having a decimal point in JSON, 8-byte double precision floating point number in Drill</td>
  </tr>
  <tr>
    <td>VARCHAR</td>
    <td>String</td>
    <td>Character string of variable length</td>
  </tr>
</table>

JSON does not enforce types or distinguish between integers and floating point values. When reading numerical values from a JSON file, Drill distinguishes integers from floating point numbers by the presence or lack of a decimal point. If some numbers in a JSON map or array appear with and without a decimal point, such as 0 and 0.0, Drill throws a schema change error.

### Handling Type Differences
Use all text mode to prevent the schema change error described in the previous section. Set the `store.json.all_text_mode` property to true.

    ALTER SYSTEM SET `store.json.all_text_mode` = true;

When you set this option, Drill reads all data from the JSON files as VARCHAR. After reading the data, use a SELECT statement in Drill to cast data as follows:

* Cast JSON numeric values to [SQL types](/docs/data-types), such as BIGINT, DECIMAL, FLOAT, INTEGER, and SMALLINT.
* Cast JSON strings to [Drill Date/Time Data Type Formats](/docs/supported-date-time-data-type-formats).

Drill uses [map and array data types](/docs/data-types) internally for reading and writing complex and nested data structures from JSON. You can cast data in a map or array of data to return a value from the structure, as shown in [“Create a view on a MapR-DB table”] (/docs/lession-2-run-queries-with-ansi-sql). “Query Complex Data” shows how to access nested arrays, for example.

## Reading JSON
To read JSON data using Drill, use a [file system storage plugin](/docs/connect-to-a-data-source) that defines the JSON format. You can use the `dfs` storage plugin, which includes the definition. 

JSON data is often complex. Data can be deeply nested and semi-structured. but [you can use workarounds ](/docs/json-data-model#limitations-and-workaroumds) covered later.

Drill reads tuples defined in single objects, having no comma between objects. A JSON object is an unordered set of name/value pairs. Curly braces delimit objects in the JSON file:

    { name: "Apples", desc: "Delicious" }
    { name: "Oranges", desc: "Florida Navel" }
    
To read and [analyze complex JSON](/docs/json-data-model#analyzing-json) files, use the FLATTEN and KVGEN functions. Observe the following guidelines when reading JSON files:

* Avoid queries that return objects larger than ??MB (16?).
  These queries might be far less performant than those that return smaller objects.
* Avoid queries that return portions of objects beyond the ??MB threshold. (16?)
  These queries might be far less performant than queries that return ports of objects within the threshold.


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
       
* Set the output format to JSON. For example:

        ALTER SESSION SET `store.format`='json';
    
* Use the path to the workspace location in a CTAS command. for example:

        USE myplugin.myworkspace;
        CREATE TABLE my_json AS
        SELECT my column from dfs.`<path_file_name>`;

Drill performs the following actions, as shown in the complete [CTAS command example](/docs/create-table-as-ctas-command):
   
* Creates a directory using table name.
* Writes the JSON data to the directory in the workspace location.


## Analyzing JSON

Generally, you query JSON files using the following syntax, which includes a table qualifier. The qualifier is typically required for querying complex data:

* Dot notation to drill down into a JSON map.

        SELECT t.level1.level2. . . . leveln FROM <storage plugin location>`myfile.json` t
        
* Use square brackets, array-style notation to drill down into a JSON array.

        SELECT t.level1.level2[n][2] FROM <storage plugin location>`myfile.json` t;
    
  The first index position of an array is 0.

Drill returns null when a document does not have the specified map or level.

Using the following techniques, you can query complex, nested JSON:

* Flatten nested data 
* Generate key/value pairs for loosely structured data

## Example: Flatten and Generate Key Values for Complex JSON
This example uses the following data that represents unit sales of tickets to events that were sold over a period of for several days in different states:

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

    SELECT * FROM dfs.`/Users/drilluser/ticket_sales.json`;
	+------------+------------+------------+------------+------------+
	|    type    |  channel   |   month    |    day     |   sales    |
	+------------+------------+------------+------------+------------+
	| ticket     | 123455     | 12         | ["15","25","28","31"] | {"NY":"532806","PA":"112889","TX":"898999","UT":"10875"} |
	| ticket     | 123456     | 12         | ["10","15","19","31"] | {"NY":"972880","PA":"857475","CA":"87350","OR":"49999"} |
	+------------+------------+------------+------------+------------+
	2 rows selected (0.041 seconds)

### Flatten JSON Data
The flatten function breaks the following _day arrays from the JSON example file shown earlier into separate rows.

    "_day": [ 15, 25, 28, 31 ] 
    "_day": [ 10, 15, 19, 31 ]

Flatten the sales column of the ticket data onto separate rows, one row for each day in the array, for a better view of the data. Flatten copies the sales data related in the JSON object on each row.  Using the all (*) wildcard as the argument to flatten is not supported and returns an error.

SELECT flatten(tkt._day) AS `day`, tkt.sales FROM dfs.`/Users/drilluser/ticket_sales.json` tkt;
    +------------+------------+
	|    day     |   sales    |
	+------------+------------+
	| 15         | {"NY":532806,"PA":112889,"TX":898999,"UT":10875} |
	| 25         | {"NY":532806,"PA":112889,"TX":898999,"UT":10875} |
	| 28         | {"NY":532806,"PA":112889,"TX":898999,"UT":10875} |
	| 31         | {"NY":532806,"PA":112889,"TX":898999,"UT":10875} |
	| 10         | {"NY":972880,"PA":857475,"CA":87350,"OR":49999} |
	| 15         | {"NY":972880,"PA":857475,"CA":87350,"OR":49999} |
	| 19         | {"NY":972880,"PA":857475,"CA":87350,"OR":49999} |
	| 31         | {"NY":972880,"PA":857475,"CA":87350,"OR":49999} |
	+------------+------------+
	8 rows selected (0.072 seconds)

### Generate Key/Value Pairs
Use the kvgen (Key Value Generator) function to generate key/value pairs from complex data. Generating key/value pairs is often helpful when working with data that contains arbitrary maps consisting of dynamic and unknown element names, such as the ticket sales data by state. For example purposes, take a look at how kvgen breaks the sales data into keys and values representing the states and number of tickets sold:

    SELECT kvgen(tkt.sales) AS state_sales FROM dfs.`/Users/drilluser/ticket_sales.json` tkt;
	+-------------+
	| state_sales |
	+-------------+
	| [{"key":"NY","value":532806},{"key":"PA","value":112889},{"key":"TX","value":898999},{"key":"UT","value":10875}] |
	| [{"key":"NY","value":972880},{"key":"PA","value":857475},{"key":"CA","value":87350},{"key":"OR","value":49999}] |
	+-------------+
	2 rows selected (0.039 seconds)

The purpose of using kvgen function is to allow queries against maps where the keys themselves represent data rather than a schema, as shown in the next example.

### Flatten JSON Data

`Flatten` breaks the list of key-value pairs into separate rows on which you can apply analytic functions. The flatten function takes a JSON array, such as the output from kvgen(sales), as an argument. Using the all (*) wildcard as the argument is not supported and returns an error.

	SELECT flatten(kvgen(sales)) Revenue 
	FROM dfs.`/Users/drilluser/drill/apache-drill-0.8.0-SNAPSHOT/ticket_sales.json`;
	+--------------+
	|   Revenue    |
	+--------------+
	| {"key":"12-10","value":532806} |
	| {"key":"12-11","value":112889} |
	| {"key":"12-19","value":898999} |
	| {"key":"12-21","value":10875} |
	| {"key":"12-10","value":87350} |
	| {"key":"12-19","value":49999} |
	| {"key":"12-21","value":857475} |
	| {"key":"12-15","value":972880} |
	+--------------+
	8 rows selected (0.171 seconds)

### Example: Aggregate Loosely Structured Data
Use flatten and kvgen together to analyze the data. Continuing with the previous example, make sure all text mode is set to false to sum numerical values. Drill returns an error if you attempt to sum data in in all text mode. 

    ALTER SYSTEM SET `store.json.all_text_mode` = false;
    
Sum the ticket sales by combining the `sum`, `flatten`, and `kvgen` functions in a single query.

    SELECT SUM(tkt.tot_sales.`value`) AS TotalSales FROM (SELECT flatten(kvgen(sales)) tot_sales FROM dfs.`/Users/drilluser/ticket_sales.json`) tkt;

    +------------+
	| TotalSales |
	+------------+
	| 3523273    |
	+------------+
	1 row selected (0.081 seconds)

### Example: Aggregate and Sort Data
Sum the ticket sales by state and group by state and sort in ascending order. 

    SELECT `right`(tkt.tot_sales.key,2) State, 
    SUM(tkt.tot_sales.`value`) AS TotalSales 
    FROM (SELECT flatten(kvgen(sales)) tot_sales 
    FROM dfs.`/Users/drilluser/ticket_sales.json`) tkt 
    GROUP BY `right`(tkt.tot_sales.key,2) 
    ORDER BY TotalSales;

	+---------------+--------------+
	| December_Date | Revenue      |
	+---------------+--------------+
	| 11            | 112889       |
	| 10            | 620156       |
	| 21            | 868350       |
	| 19            | 948998       |
	| 15            | 972880       |
	+---------------+--------------+
	5 rows selected (0.203 seconds)

### Example: Analyze a Map Field in an Array
To access a map field in an array, use dot notation to drill down through the hierarchy of the JSON data to the field. Examples are based on the following [City Lots San Francisco in .json](https://github.com/zemirco/sf-city-lots-json), modified slightly as described in the empty array workaround in ["Limitations and Workarounds."](/docs/json-data-model#empty-array)

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
   { 
      "type": "Feature", 
   . . .

This example shows you how to drill down using array notation plus dot notation in features[0].properties.MAPBLKLOT to get the MAPBLKLOT property value in the San Francisco city lots data:

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
		+------------+
		|   EXPR$0   |
		+------------+
		| 37.80848009696725 |
		+------------+
		1 row selected (0.19 seconds)

More examples of drilling down into an array are shown in ["Selecting Nested Data for a Column"](/docs/query-3-selecting-nested-data-for-a-column). 

### Example: Flatten an Array of Maps using a Subquery
By flattening the following JSON file, which contains an array of maps, you can evaluate the records of the flattened data. 

    {"name":"classic","fillings":[ {"name":"sugar","cal":500} , {"name":"flour","cal":300} ] }

    SELECT flat.fill FROM (SELECT flatten(t.fillings) AS fill FROM dfs.flatten.`test.json` t) flat WHERE flat.fill.cal  > 300;

    +------------+
	|    fill    |
	+------------+
	| {"name":"sugar","cal":500} |
	+------------+
	1 row selected (0.421 seconds)

Use a table qualifier for column fields and functions when working with complex data sets. Currently, you must use a subquery when operating on a flattened column. Eliminating the subquery and table qualifier in the WHERE clause, for example `flat.fillings[0].cal > 300`, does not evaluate all records of the flattened data against the predicate and produces the wrong results.

### Example: Analyze Map Fields in a Map
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

+------------+------------+
|    Name    |   Weight   |
+------------+------------+
| Thongneoupeanu | CATEGORY_2 |
+------------+------------+
1 row selected (0.142 seconds)

## Limitations and Workarounds
In most cases, you can use a workaround, presented in the following sections, to overcome the following limitations:

* Array at the root level
* Complex nested data
* Empty array
* Lengthy JSON objects
* Complex JSON objects
* Nested column names
* Schema changes
* Selecting all in a JSON directory query 

### Array at the root level
Drill cannot read an array at the root level, outside an object.

Workaround: Remove square brackets at the root of the object, as shown in the following example.

![drill query flow]({{ site.baseurl }}/docs/img/datasources-json-bracket.png)

### Complex nested data
Drill cannot read some complex nested arrays unless you use a table qualifier.

Workaround: To query n-level nested data, use the table qualifier to remove ambiguity; otherwise, column names such as user_info are parsed as table names by the SQL parser. The qualifier is not needed for data that is not nested, as shown in the following example:

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
<<Jason will try to provide some statement about limits.>>

### Complex JSON objects
Complex arrays and maps can be difficult or impossible to query.

Workaround: 

Separate lengthy objects into objects delimited by curly braces using the following functions:
 
[flatten](/docs/json-data-model#flatten-json-data) separates a set of nested JSON objects into individual rows in a DRILL table.
[kvgen](/docs/json-data-model#generate-key-value-pairs) separates objects having more elements than optimal for querying.

  
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
                
Workaround: Set the `store.json.all_text_mode` property, described earlier, to true.

    ALTER SYSTEM SET `store.json.all_text_mode` = true;

### Selecting all in a JSON directory query
Drill currently returns only fields common to all the files in a [directory query](/docs/lesson-3-create-a-storage-plugin#query-multiple-files-in-a-directory) that selects all (SELECT *) JSON files.

Workaround: Query each file individually.





