---
title: "SELECT"
date: 2018-12-04
parent: "SQL Commands"
---
Drill supports the following ANSI standard clauses in the SELECT statement:

  * WITH clause
  * SELECT list
  * FROM clause
  * WHERE clause
  * GROUP BY clause
  * HAVING clause
  * UNION and UNION ALL set operators
  * ORDER BY clause (with an optional LIMIT clause)
  * Limit clause
  * Offset clause

You can use the same SELECT syntax in the following commands:

  * CREATE TABLE AS (CTAS)
  * CREATE TEMPORARY TABLES AS (CTTAS)
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

## Joins

Drill supports ANSI standard joins in the FROM and WHERE clauses:

  * Inner joins
  * Left, full, and right outer joins
  * Cross joins (as of Drill 1.15)

The following types of join syntax are supported:

| Join type                      | Syntax                                             |
|--------------------------------|----------------------------------------------------|
| Join condition in WHERE clause | FROM table1, table 2 WHERE table1.col1=table2.col1 |
| ON join in FROM clause         | FROM table1 JOIN table2 ON table1.col1=table2.col1 |


You must specify a join condition when more than one table is listed in the FROM clause.

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

