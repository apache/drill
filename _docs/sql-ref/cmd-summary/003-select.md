---
title: "SELECT Statements"
parent: "SQL Commands Summary"
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

