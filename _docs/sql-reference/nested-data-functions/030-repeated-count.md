---
title: "REPEATED_COUNT"
date: 2018-11-02
parent: "Nested Data Functions"
---
This function counts the values in an array. 

## Syntax

    REPEATED_COUNT (array)

*array* is the name of an array.

## Usage Notes

The COUNT function requires a single argument, which must be an array. Note that
this function is not a standard SQL aggregate function and does not require
the count to be grouped by other columns in the select list (such as `name` in
this example). 

## Example
The following example returns the
counts for the `categories` array in the `yelp_academic_dataset_business.json`
file. The counts are restricted to rows that contain the string `pizza`.

	SELECT name, REPEATED_COUNT(categories) 
	FROM   dfs.yelp.`yelp_academic_dataset_business.json` 
	WHERE  name LIKE '%pizza%';
	 
	+---------------+------------+
	|    name       |   EXPR$1   |
	+---------------+------------+
	| Villapizza    | 2          |
	| zpizza        | 4          |
	| zpizza        | 4          |
	| Luckys pizza  | 2          |
	| Zpizza        | 2          |
	| S2pizzabar    | 4          |
	| Dominos pizza | 5          |
	+---------------+------------+
	 
	7 rows selected (2.03 seconds)

For another example of this function, see the following lesson in the Apache
Drill Tutorial for Hadoop: [Lesson 3: Run Queries on Complex Data Types]({{ site.baseurl }}/docs/lesson-3-run-queries-on-complex-data-types/).
