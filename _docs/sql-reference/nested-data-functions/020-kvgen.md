---
title: "KVGEN"
date: 2018-11-02
parent: "Nested Data Functions"
---
Returns a list of the keys that exist in the map.

## Syntax

    KVGEN(column)

*column* is the name of a column.

## Usage Notes

You use KVGEN (Key-Value Generation) to query maps that have keys instead of a schema to
represent data. For example, you store statistics
about the number of interactions between the users on a social network
in a JSON document store. User records appear as follows:

	{
	   user_id : 12345,
	   user_interactions : {
	       "12633" : 10,
	       "25678" : 25,
	       "11111" : 5
	   }
	}

This record summarizes the interaction of one user with others. The record contains the user_id and a map between other
users' ids and the number of interactions recorded between them. A complete dataset stores the summary of user interactions and includes 12345 listed under user_interactions for user records 12633 and 25678. For example:

	{
	        user_id: 12633,
	        user_interactions : {
	            "12345" : 10,
	            "27569" : 104,
	            "93033" : 52
	    }
	}
	{       user_id: 25678,
	        user_interactions : {
	            "12345" : 25,
	            "37886" : 14,
	            "87394" : 5
	    }
	}

To list the users that interact most, you need to use a subquery; otherwise, Drill operates on the data before the flattening and key generation occurs:

    SELECT t.flat_interactions.key, t.flat_interactions.`value` from (select flatten(kvgen(user_interactions)) as flat_interactions from dfs.`/drilluser/user_table.json`) as t order by t.flat_interactions.`value` DESC;

    +------------+------------+
	|   EXPR$0   |   EXPR$1   |
	+------------+------------+
	| 27569      | 104        |
	| 93033      | 52         |
	| 25678      | 25         |
	| 12345      | 25         |
	| 37886      | 14         |
	| 12633      | 10         |
	| 12345      | 10         |
	| 11111      | 5          |
	| 87394      | 5          |
	+------------+------------+
	9 rows selected (0.093 seconds)


KVGEN is useful when complex
data files contain arbitrary maps that consist of relatively "unknown" column
names. Instead of having to specify columns in the map to access the data, you
can use KVGEN to return a list of the keys that exist in the map. KVGEN turns
a map with a wide set of columns into an array of key-value pairs.

In turn, you can write analytic queries that return a subset of the generated
keys or constrain the keys in some way. For example, you can use the
[FLATTEN]({{ site.baseurl }}/docs/flatten) function to break the
array down into multiple distinct rows and further query those rows.

For example, assume that a JSON file named `simplemaps.json` contains this data:  

	{"rec1":{"a": "valA", "b": "valB"}}
	{"rec1":{"c": "valC", "d": "valD"}}

KVGEN would operate on this data as follows:

	SELECT KVGEN(rec1) FROM `simplemaps.json`;
	+------------+
	|   EXPR$0   |
	+------------+
	| [{"key":"a","value":"valA"},{"key":"b","value":"valB"}] |
	| [{"key":"c","value":"valC"},{"key":"d","value":"valD"}] |
	+------------+
	2 rows selected (0.201 seconds)

Applying the FLATTEN function to this data would return:

    {"key": "a", "value": "valA"}
    {"key": "b", "value": "valB"}
    {"key": "c", "value": "valC"}
    {"key": "d", "value": "valD"}

## Example: Different Data Type Values

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

    0: jdbc:drill:zk=local> select * from dfs.`kvgendata.json` where rownum=1;
 
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


For more examples of KVGEN and FLATTEN, see the examples in the section, ["JSON Data Model"]({{ site.baseurl }}/docs/json-data-model).
