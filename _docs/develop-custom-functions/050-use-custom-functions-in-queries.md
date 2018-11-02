---
title: "Using Custom Functions in Queries"
date: 2018-11-02
parent: "Develop Custom Functions"
---
When you issue a query with a custom function to Drill, Drill searches the
classpath for the function that matches the request in the query. Once Drill
locates the function for the request, Drill processes the query and applies
the function during processing.

Your Drill installation includes sample files in the Drill classpath. One
sample file, [`employee.json`]({{site.baseurl}}/docs/querying-json-files/), contains some fictitious employee data that you
can query with a custom function.

## Simple Function Example

This example uses the `myaddints` simple function in a query on the
`employee.json` file.

If you issue the following query to Drill, you can see all of the employee
data within the `employee.json` file:

    0: jdbc:drill:zk=local> SELECT * FROM cp.`employee.json`;

The query returns the following results:

	| employee_id | full_name    | first_name | last_name  | position_id | position_title          |  store_id  | department_id | birth_da |
	+-------------+------------+------------+------------+-------------+----------------+------------+---------------+----------+-----------
	| 1101        | Steve Eurich | Steve      | Eurich     | 16          | Store Temporary Checker | 12         | 16            |
	| 1102        | Mary Pierson | Mary       | Pierson    | 16          | Store Temporary Checker | 12         | 16            |
	| 1103        | Leo Jones    | Leo        | Jones      | 16          | Store Temporary Checker | 12         | 16            |
	…

Since the `postion_id` and `store_id` columns contain integers, you can issue
a query with the `myaddints` custom function on these columns to add the
integers in the columns.

The following query tells Drill to apply the `myaddints` function to the
`position_id` and `store_id` columns in the `employee.json` file:

    0: jdbc:drill:zk=local> SELECT myaddints(CAST(position_id AS int),CAST(store_id AS int)) FROM cp.`employee.json`;

Since JSON files do not store information about data types, you must apply the
`CAST` function in the query to tell Drill that the columns contain integer
values.

The query returns the following results:

	+------------+
	|   EXPR$0   |
	+------------+
	| 28         |
	| 28         |
	| 36         |
	+------------+
	…
