
title: "JSON Data Model"
parent: "Data Sources"
---
Drill supports [JSON (JavaScript Object Notation)](http://www.json.org/), a self-describing data format. The data itself implies its schema and has the following characteristics:

* Language-independent
* Textual format
* Loosely defined, weak data typing

Semi-structured JSON data often consists of complex, nested elements having schema-less fields that differ type-wise from row to row. The data can constantly evolve. Applications typically add and remove fields frequently to meet business requirements.

Using Drill you can natively query dynamic JSON data sets using SQL. Drill treats a JSON object as a SQL record. One object equals one row in a Drill table. 

Using Drill you can natively query dynamic JSON data sets using SQL. Drill treats a JSON object as a SQL record. One object equals one row in a Drill table. 

Drill 0.8 and higher can  query compressed .gz files having JSON as well as uncompressed .json files.<<link to section>>.

n addition to the examples presented later in this section, see "How to Analyze Highly Dynamic Datasets with Apache Drill" (https://www.mapr.com/blog/how-analyze-highly-dynamic-datasets-apache-drill) for information about how to analyze a JSON data set.

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

* Cast [JSON numeric values](/docs/lession-2-run-queries-with-ansi-sql#return-customer-data-with-appropriate-data-types) to SQL types, such as BIGINT, DECIMAL, FLOAT, INTEGER, and SMALLINT.
* Cast JSON strings to [Drill Date/Time Data Type Formats](/docs/supported-date-time-data-type-formats).

For example, apply a [Drill view] (link to view reference) to the data. 

Drill uses [map and array data types](/docs/data-types) internally for reading and writing complex and nested data structures from JSON. <<true?>>

## Reading JSON
To read JSON data using Drill, use a [file system storage plugin](link to plugin section) that defines the JSON format. You can use the `dfs` storage plugin, which includes the definition. 

JSON data is often complex. Data can be deeply nested and semi-structured. but [you can use workarounds ](link to section) covered later.

Drill reads tuples defined in single objects, having no comma between objects. A JSON object is an unordered set of name/value pairs. Curly braces delimit objects in the JSON file:

    { name: "Apples", desc: "Delicious" }
    { name: "Oranges", desc: "Florida Navel" }
    
To read and [analyze complex JSON](link to Analyzing JSON) files, use the FLATTEN and KVGEN functions. Observe the following guidelines when reading JSON files:

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
   
Observe the following size limitations pertaining to JSON objects:

* Objects must be smaller than the chunk size.
* Objects must be smaller than ?GB (2?) on 32- and some 64-bit systems.
* Objects must be smaller than the amount of memory available to Drill.

## Analyzing JSON

Generally, you query JSON files using the following syntax:

* Dot notation to drill down into a JSON map.

        SELECT level1.level2. . . . leveln FROM <storage plugin location>`myfile.json`
        
* Use square brackets, array-style notation to drill down into a JSON array.

        SELECT level1.level2[n][2] FROM <storage plugin location>`myfile.json`;
    
  The first index position of an array is 0.

Using the following techniques, you can query complex, nested JSON:

* Generate key/value pairs for loosely structured data
* Flatten nested data 

### Generate Key/Value Pairs
Use the ‘KVGen’ (Key Value Generator) with complex data that contains arbitrary maps consisting of dynamic and unknown element names, such as ticket_info in the following example:

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
    

This query reads the data, and the output shows how Drill restructures it:

    SELECT * FROM dfs.`/Users/drilluser/drill/apache-drill-0.8.0-SNAPSHOT/ticket_sales.json`;
    
    +------------+------------+------------+
	|    type    |   venue    |   sales    |
	+------------+------------+------------+
	| ticket     | 123455     | {"12-10":532806,"12-11":112889,"12-19":898999,"12-21":10875} |
	| ticket     | 123456     | {"12-10":87350,"12-19":49999,"12-21":857475,"12-15":972880} |
	+------------+------------+------------+
	2 rows selected (0.895 seconds)

`KVGen` turns the dynamic map into an array of key-value pairs where keys represent the dynamic element names.

    SELECT kvgen(sales) Revenue FROM dfs.`/Users/drilluser/drill/apache-drill-0.8.0-SNAPSHOT/ticket_sales.json`;
    
	+--------------+
	|   Revenue    |
	+--------------+
	| [{"key":"12-10","value":532806},{"key":"12-11","value":112889},{"key":"12-19","value":898999},{"key":"12-21","value":10875}] |
	| [{"key":"12-10","value":87350},{"key":"12-19","value":49999},{"key":"12-21","value":857475},{"key":"12-15","value":972880}] |
	+--------------+
	2 rows selected (0.341 seconds)

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
Continuing with the previous example, make sure all text mode is set to false to sum numerical values. 

    ALTER SYSTEM SET `store.json.all_text_mode` = false;
    
Sum the ticket sales by combining the `sum`, `flatten`, and `kvgen` functions in a single query.

    SELECT sum(tickettbl.tickets.`value`) AS Revenue 
    FROM (SELECT flatten(kvgen(sales)) tickets 
    FROM  dfs.`/Users/drilluser/drill/apache-drill-0.8.0-SNAPSHOT/ticket_sales.json` ) tickettbl;
    
	+------------+
	|  Revenue   |
	+------------+
	| 3523273    |
	+------------+
	1 row selected (0.194 seconds)


### Example: Aggregate and Sort Data
Sum the ticket sales for each date in December, and sort by total sales in ascending order.

    SELECT `right`(tickettbl.tickets.key,2) December_Date, 
    sum(tickettbl.tickets.`value`) Revenue 
    FROM (select flatten(kvgen(sales)) tickets 
    FROM dfs.`/Users/drilluser/drill/apache-drill-0.8.0-SNAPSHOT/ticket_sales.json`) tickettbl
    GROUP BY `right`(tickettbl.tickets.key,2) 
    ORDER BY Revenue;

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
To access a map field in an array, use dot notation to drill down through the hierarchy of the JSON data to the field. The following example shows how to drill down to get the MAPBLKLOT property value the [City Lots San Francisco in .json](https://github.com/zemirco/sf-city-lots-json).

![drill query flow]({{ site.baseurl }}/docs/img/json-workaround.png)

        SELECT features[0].properties.MAPBLKLOT,  
        FROM <storage location>.`citylots.json`;
          
        +------------+
		|   EXPR$0   |
		+------------+
		| 0001001    |
		+------------+
		1 row selected (0.163 seconds)
		
To access the second geometry coordinate of the first city lot in the San Francisco city lots, use dot notation and array indexing notation:
		
		SELECT features[0].geometry.coordinates[0][1] 
		FROM <storage location>.`citylots.json`;
		+------------+
		|   EXPR$0   |
		+------------+
		| 37.80848009696725 |
		+------------+
		1 row selected (0.19 seconds)

More examples of drilling down into an array are shown in ["Selecting Nested Data for a Column"](/docs/query-3-selecting-nested-data-for-a-column). 

### Example: Analyze Map Fields in a Map
This example uses a WHERE clause to drill down to a third level of the following JSON hierarchy to get the Id and weight of the person whose max_hdl exceeds 160, use dot notation as shown in the query that follows:

    {
	    "SOURCE": "Allegheny County",
	    "TIMESTAMP": 1366369334989,
	    "birth": {
	        "id": 35731300,
	        "dur": 215923,
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
	} . . .

	SELECT tbl.birth.id AS Id, tbl.birth.weight AS Weight 
	FROM dfs.`/Users/drilluser/drill/vitalstat.json` AS tbl 
	WHERE tbl.birth.id IN (
	SELECT tbl1.birth.id 
	FROM dfs.`/Users/drilluser/drill/vitalstat.json` AS tbl1 
	WHERE tbl1.birth.bearer.max_hdl > 160); 
	
	+------------+------------+
	|     Id     |   Weight   |
	+------------+------------+
	| 35731300   | CATEGORY_1 |
	+------------+------------+
	1 row selected (1.424 seconds)

## Querying Compressed JSON

You can use Drill 0.8 and later to query compressed JSON in .gz files as well as uncompressed files having the .json extension as described in Reading and Writing JSON Files<<link to section>>. First, add the gz extension to a storage plugin, and then use that plugin to query the compressed file.

      "extensions": [
        "json",
        "gz"
      ]
<<Is this going to be in 0.8?>>

## Limitations and Workarounds
In most cases, you can use a workaround, presented in the following sections, to overcome the following limitations:

* Array at the root level
* Complex nested data
* Empty array
* Lengthy JSON objects
* Nested column names
* Schema changes
* Selecting all in a JSON directory query 

### Array at the root level
Drill cannot read an array at the root level, outside an object.

Workaround: Remove square brackets at the root of the object.

### Complex nested data
Drill cannot read some complex nested arrays unless you use a table qualifier.

Workaround: To query n-level nested data, use table alias to remove ambiguity. The table alias is required; otherwise column names such as user_info are parsed as table names by the SQL parser. The qualifier is not needed for data that is not nested, as shown in the following example:

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

Drilling down into lengthy JSON objects, having just a few or a single set of curly braces, requires flattening and generation of keys.

Workaround: 

Separate lengthy objects into many objects delimited by curly braces using the following functions:
 
  * FLATTEN <<link to example>> separates a set of nested JSON objects into individual rows in a DRILL table.
  * KVGEN <<link to example>> separates objects having more elements than optimal for querying.
  
### Nested Column Names 

You cannot use reserved words for nested column names because Drill returns null if you enclose n-level nested column names in back ticks. The previous example encloses the date and time column names in back ticks because the names are reserved words. The enclosure of column names in back ticks works because the date and time columns belong to the first level of the JSON object.

### Schema changes
Drill cannot read JSON files containing changes in the schema. For example, attempting to query an object having array elements of different data types cause an error:

        . . .
            "geometry": {
                 "type": "Polygon",
                 "coordinates": [
                   [
                     -122.42200352825247,
                     37.80848009696725,
                     0
                   ],
        . . .
Drill interprets numbers that do not have a decimal point as BigInt values. In this example, Drill recognizes the first two coordinates as doubles and the third coordinate as a BigInt, which causes an error. 
                
Workaround: Set the `store.json.all_text_mode` property, described earlier, to true.

    ALTER SYSTEM SET `store.json.all_text_mode` = true;

### Selecting all in a JSON directory query
Drill currently returns only fields common to all the files in a [directory query](link to basics tutorial) that selects all (SELECT *) JSON files.

Workaround: Query each file individually.





