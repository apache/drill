---
title: "WITH Clause"
date: 2017-04-05 00:09:58 UTC
parent: "SQL Commands"
---
The WITH clause is an optional clause used to contain one or more common table
expressions (CTE) where each CTE defines a temporary table that exists for the
duration of the query. Each subquery in the WITH clause specifies a table
name, an optional list of column names, and a SELECT statement.

## Syntax

The WITH clause supports the following syntax:

    WITH with_subquery [, ...]

where with_subquery is:

    with_subquery_table_name [ ( column_name [, ...] ) ] AS ( query ) 

## Parameters

*with_subquery_table_name*  
A unique name for a temporary table that defines the results of a WITH clause
subquery. You cannot use duplicate names within a single WITH clause. You must
give each subquery a table name that can be referenced in the FROM clause.

*column_name*  
An optional list of output column names for the WITH clause subquery,
separated by commas. The number of column names specified must be equal to or
less than the number of columns defined by the subquery.

*query*  
Any SELECT query that Drill supports. See
[SELECT]({{ site.baseurl }}/docs/select/).

## Usage Notes

Use the WITH clause to efficiently define temporary tables that Drill can
access throughout the execution of a single query. The WITH clause is
typically a simpler alternative to using subqueries in the main body of the
SELECT statement. In some cases, Drill can evaluate a WITH subquery once and
reuse the results for query optimization.

You can use a WITH clause in the following SQL statements:

  * SELECT (including subqueries within SELECT statements)  
  * CREATE TABLE AS
  * CREATE VIEW
  * EXPLAIN

You can reference the temporary tables in the FROM clause of the query. If the
FROM clause does not reference any tables defined by the WITH clause, Drill
ignores the WITH clause and executes the query as normal.

Drill can only reference a table defined by a WITH clause subquery in the
scope of the SELECT query that the WITH clause begins. For example, you can
reference such a table in the FROM clause of a subquery in the SELECT list,
WHERE clause, or HAVING clause. You cannot use a WITH clause in a subquery and
reference its table in the FROM clause of the main query or another subquery.

You cannot specify another WITH clause inside a WITH clause subquery.

For example, the following query includes a forward reference to table t2 in
the definition of table t1:

## Example

The following example shows the WITH clause used to create a WITH query named
`emp_data` that selects all of the rows from the `employee.json` file. The
main query selects the `full_name, position_title, salary`, and `hire_date`
rows from the `emp_data` temporary table (created from the WITH subquery) and
orders the results by the hire date. The `emp_data` table only exists for the
duration of the query.

**Note:** The `employee.json` file is included with the Drill installation. It is located in the `cp.default` workspace which is configured by default. 

       0: jdbc:drill:zk=local> WITH emp_data AS (SELECT * FROM cp.`employee.json`) 
       SELECT full_name, position_title, salary, hire_date 
       FROM emp_data ORDER BY hire_date LIMIT 10;
    +------------------+-------------------------+------------+-----------------------+
    | full_name        | position_title          |   salary   | hire_date             |
    +------------------+-------------------------+------------+-----------------------+
    | Bunny McCown     | Store Assistant Manager | 8000.0     | 1993-05-01 00:00:00.0 |
    | Danielle Johnson | Store Assistant Manager | 8000.0     | 1993-05-01 00:00:00.0 |
    | Dick Brummer     | Store Assistant Manager | 7900.0     | 1993-05-01 00:00:00.0 |
    | Gregory Whiting  | Store Assistant Manager | 10000.0    | 1993-05-01 00:00:00.0 |
    | Juanita Sharp    | HQ Human Resources      | 6700.0     | 1994-01-01 00:00:00.0 |
    | Sheri Nowmer     | President               | 80000.0    | 1994-12-01 00:00:00.0 |
    | Rebecca Kanagaki | VP Human Resources      | 15000.0    | 1994-12-01 00:00:00.0 |
    | Shauna Wyro      | Store Manager           | 15000.0    | 1994-12-01 00:00:00.0 |
    | Roberta Damstra  | VP Information Systems  | 25000.0    | 1994-12-01 00:00:00.0 |
    | Pedro Castillo   | VP Country Manager      | 35000.0    | 1994-12-01 00:00:00.0 |
    +------------+----------------+--------------+------------------------------------+
