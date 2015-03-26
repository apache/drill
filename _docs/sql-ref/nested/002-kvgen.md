---
title: "KVGEN Function"
parent: "Nested Data Functions"
---
Return a list of the keys that exist in the map.

## Syntax

    KVGEN(column)

*column* is the name of a column.

## Usage Notes

KVGEN stands for _key-value generation_. This function is useful when complex
data files contain arbitrary maps that consist of relatively "unknown" column
names. Instead of having to specify columns in the map to access the data, you
can use KVGEN to return a list of the keys that exist in the map. KVGEN turns
a map with a wide set of columns into an array of key-value pairs.

In turn, you can write analytic queries that return a subset of the generated
keys or constrain the keys in some way. For example, you can use the
[FLATTEN](/docs/flatten-function) function to break the
array down into multiple distinct rows and further query those rows.

For example, assume that a JSON file contains this data:  

    {"a": "valA", "b": "valB"}
    {"c": "valC", "d": "valD"}
  

KVGEN would operate on this data to generate:

    [{"key": "a", "value": "valA"}, {"key": "b", "value": "valB"}]
    [{"key": "c", "value": "valC"}, {"key": "d", "value": "valD"}]

Applying the [FLATTEN](/docs/flatten-function) function to
this data would return:

    {"key": "a", "value": "valA"}
    {"key": "b", "value": "valB"}
    {"key": "c", "value": "valC"}
    {"key": "d", "value": "valD"}

Assume that a JSON file called `kvgendata.json` includes multiple records that
look like this one:

    {
	    "rownum": 1,
	    "bigintegercol": {
	        "int_1": 1,
	        "int_2": 2,
	        "int_3": 3
	    },
	    "varcharcol": {
	        "varchar_1": "abc",
	        "varchar_2": "def",
	        "varchar_3": "xyz"
	    },
	    "boolcol": {
	        "boolean_1": true,
	        "boolean_2": false,
	        "boolean_3": true
	    },
	    "float8col": {
	        "f8_1": 1.1,
	        "f8_2": 2.2
	    },
	    "complex": [
	        {
	            "col1": 3
	        },
	        {
	            "col2": 2,
	            "col3": 1
	        },
	        {
	            "col1": 7
	        }
	    ]
    }
 
	{
	    "rownum": 3,
	    "bigintegercol": {
	        "int_1": 1,
	        "int_3": 3
	    },
	    "varcharcol": {
	        "varchar_1": "abcde",
	        "varchar_2": null,
	        "varchar_3": "xyz",
	        "varchar_4": "xyz2"
	    },
	    "boolcol": {
	        "boolean_1": true,
	        "boolean_2": false
	    },
	    "float8col": {
	        "f8_1": 1.1,
	        "f8_3": 6.6
	    },
	    "complex": [
	        {
	            "col1": 2,
	            "col3": 1
	        }
	    ]
	}
	...


A SELECT * query against this specific record returns the following row:

    0: jdbc:drill:zk=local> select * from dfs.yelp.`kvgendata.json` where rownum=1;
 
	+------------+---------------+------------+------------+------------+------------+
	|   rownum   | bigintegercol | varcharcol |  boolcol   | float8col  |  complex   |
	+------------+---------------+------------+------------+------------+------------+
	| 1          | {"int_1":1,"int_2":2,"int_3":3} | {"varchar_1":"abc","varchar_2":"def","varchar_3":"xyz"} | {"boolean_1":true,"boolean_2":false,"boolean_3":true} | {"f8_1":1.1,"f8_2":2.2} | [{"col1":3},{"col2":2,"col3":1},{"col1":7}] |
	+------------+---------------+------------+------------+------------+------------+
	1 row selected (0.122 seconds)

You can use the KVGEN function to turn the maps in this data into key-value
pairs. For example:

	0: jdbc:drill:zk=local> select kvgen(varcharcol) from dfs.yelp.`kvgendata.json`;
	+------------+
	|   EXPR$0   |
	+------------+
	| [{"key":"varchar_1","value":"abc"},{"key":"varchar_2","value":"def"},{"key":"varchar_3","value":"xyz"}] |
	| [{"key":"varchar_1","value":"abcd"}] |
	| [{"key":"varchar_1","value":"abcde"},{"key":"varchar_3","value":"xyz"},{"key":"varchar_4","value":"xyz2"}] |
	| [{"key":"varchar_1","value":"abc"},{"key":"varchar_2","value":"def"}] |
	+------------+
	4 rows selected (0.091 seconds)

Now you can apply the FLATTEN function to break out the key-value pairs into
distinct rows:

	0: jdbc:drill:zk=local> select flatten(kvgen(varcharcol)) from dfs.yelp.`kvgendata.json`;
	+------------+
	|   EXPR$0   |
	+------------+
	| {"key":"varchar_1","value":"abc"} |
	| {"key":"varchar_2","value":"def"} |
	| {"key":"varchar_3","value":"xyz"} |
	| {"key":"varchar_1","value":"abcd"} |
	| {"key":"varchar_1","value":"abcde"} |
	| {"key":"varchar_3","value":"xyz"} |
	| {"key":"varchar_4","value":"xyz2"} |
	| {"key":"varchar_1","value":"abc"} |
	| {"key":"varchar_2","value":"def"} |
	+------------+
	9 rows selected (0.151 seconds)

For more examples of KVGEN and FLATTEN, see the examples in the section, ["JSON Data Model"](/docs/json-data-model).