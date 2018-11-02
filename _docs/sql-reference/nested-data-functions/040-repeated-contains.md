---
title: "REPEATED_CONTAINS"
date: 2018-11-02
parent: "Nested Data Functions"
---
REPEATED CONTAINS searches for a keyword in an array. 

## Syntax

    REPEATED_CONTAINS(array_name, keyword)

* array_name is a simple array, such as topping:

		{
		. . .
		    "topping":
		        [
		            "None",
		            "Glazed",
		            "Sugar",
		            "Powdered Sugar",
		            "Chocolate with Sprinkles",
		            "Chocolate",
		            "Maple"
		        ]
		}

* keyword is a value in the array, such as 'Glazed'.

## Usage Notes
REPEATED_CONTAINS returns true if Drill finds a match; otherwise, the function returns false. The function supports regular expression wildcards, but not at the beginning of the keyword:

* Asterisk (*)
* Period (.)
* Question mark (?)
* Square bracketed ranges [a-z]
* Square bracketed characters [ch]
* Negated square bracketed ranges or characters [!ch].

Enclose keyword string values in single quotation marks. Do not enclose numerical keyword values in single quotation marks.

## Examples
The examples in this section use `testRepeatedWrite.json`. To download this file, go to [Drill test resources](https://github.com/apache/drill/tree/master/exec/java-exec/src/test/resources) page, locate testRepeatedWrite.json in the list of files, and download it.

Which donuts have glazed or glaze toppings?

		SELECT name, REPEATED_CONTAINS(topping, 'Glaze?') AS `Glazed?` FROM  dfs.`/Users/drilluser/testRepeatedWrite.json` WHERE type='donut';

		+------------+------------+
		|    name    |  Glazed?   |
		+------------+------------+
		| Cake       | true       |
		| Raised     | true       |
		| Old Fashioned | true       |
		| Filled     | true       |
		| Apple Fritter | true       |
		+------------+------------+
		5 rows selected (0.072 seconds)

Which objects have powdered sugar toppings? Use the asterisk wildcard instead of typing the entire keyword pair.

    SELECT name, REPEATED_CONTAINS(topping, 'P*r') AS `Powdered Sugar?` FROM  dfs.`/Users/drilluser/testRepeatedWrite.json` WHERE type='donut';

	+------------+-----------------+
	|    name    | Powdered Sugar? |
	+------------+-----------------+
	| Cake       | true            |
	| Raised     | true            |
	| Old Fashioned | false           |
	| Filled     | true            |
	| Apple Fritter | false           |
	+------------+-----------------+
	5 rows selected (0.089 seconds)

Which donuts have toppings beginning with the letters "Map" and ending in any two letters?

	SELECT name, REPEATED_CONTAINS(topping, 'Map..') AS `Maple?` FROM  dfs.`/Users/drilluser/testRepeatedWrite.json` WHERE type='donut';

	+------------+------------+
	|    name    |   Maple?   |
	+------------+------------+
	| Cake       | true       |
	| Raised     | true       |
	| Old Fashioned | true       |
	| Filled     | true       |
	| Apple Fritter | false      |
	+------------+------------+
	5 rows selected (0.085 seconds)


