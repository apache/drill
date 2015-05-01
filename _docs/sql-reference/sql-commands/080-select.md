---
title: "SELECT Statements"
parent: "SQL Commands"
---
Drill supports the following ANSI standard clauses in the SELECT statement:

  * WITH clause
  * SELECT list
  * FROM clause
  * WHERE clause
  * GROUP BY clause
  * HAVING clause
  * ORDER BY clause (with an optional LIMIT clause)

You can use the same SELECT syntax in the following commands:

  * CREATE TABLE AS (CTAS)
  * CREATE VIEW

INSERT INTO SELECT is not yet supported.

## Column Aliases

You can use named column aliases in the SELECT list to provide meaningful
names for regular columns and computed columns, such as the results of
aggregate functions. See the section on running queries for examples.

You cannot reference column aliases in the following clauses:

  * WHERE
  * GROUP BY
  * HAVING

Because Drill works with schema-less data sources, you cannot use positional
aliases (1, 2, etc.) to refer to SELECT list columns, except in the ORDER BY
clause.

## UNION ALL Set Operator

Drill supports the UNION ALL set operator to combine two result sets. The
distinct UNION operator is not yet supported.

The EXCEPT, EXCEPT ALL, INTERSECT, and INTERSECT ALL operators are not yet
supported.

## Joins

Drill supports ANSI standard joins in the FROM and WHERE clauses:

  * Inner joins
  * Left, full, and right outer joins

The following types of join syntax are supported:

Join type| Syntax  
---|---  
Join condition in WHERE clause|FROM table1, table 2 WHERE table1.col1=table2.col1  
USING join in FROM clause|FROM table1 JOIN table2 USING(col1, ...)  
ON join in FROM clause|FROM table1 JOIN table2 ON table1.col1=table2.col1  
NATURAL JOIN in FROM clause|FROM table 1 NATURAL JOIN table 2  

Cross-joins are not yet supported. You must specify a join condition when more
than one table is listed in the FROM clause.

Non-equijoins are supported if the join also contains an equality condition on
the same two tables as part of a conjunction:

    table1.col1 = table2.col1 AND table1.c2 < table2.c2

This restriction applies to both inner and outer joins.

## Subqueries

You can use the following subquery operators in Drill queries. These operators
all return Boolean results.

  * ALL
  * ANY
  * EXISTS
  * IN
  * SOME

In general, correlated subqueries are supported. EXISTS and NOT EXISTS
subqueries that do not contain a correlation join are not yet supported.

## WITH Clause

The WITH clause is an optional clause used to contain one or more common table
expressions (CTE) where each CTE defines a temporary table that exists for the
duration of the query. Each subquery in the WITH clause specifies a table
name, an optional list of column names, and a SELECT statement.

## Syntax

The WITH clause supports the following syntax:

    [ WITH with_subquery [, ...] ]
    where with_subquery is:
    with_subquery_table_name [ ( column_name [, ...] ) ] AS ( query ) 

## Parameters

_with_subquery_table_name_

A unique name for a temporary table that defines the results of a WITH clause
subquery. You cannot use duplicate names within a single WITH clause. You must
give each subquery a table name that can be referenced in the FROM clause.

_column_name_

An optional list of output column names for the WITH clause subquery,
separated by commas. The number of column names specified must be equal to or
less than the number of columns defined by the subquery.

_query_

Any SELECT query that Drill supports. See
[SELECT]({{ site.baseurl }}/docs/SELECT+Statements).

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

    0: jdbc:drill:zk=local> with emp_data as (select * from cp.`employee.json`) select full_name, position_title, salary, hire_date from emp_data order by hire_date limit 10;
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