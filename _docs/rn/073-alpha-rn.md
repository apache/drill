---
title: "Apache Drill M1 Release Notes (Apache Drill Alpha)"
parent: "Release Notes"
---
## Milestone 1 Goals

The first release of Apache Drill is designed as a technology preview for
people to better understand the architecture and vision. It is a functional
release tying to piece together the key components of a next generation MPP
query engine. It is designed to allow milestone 2 (M2) to focus on
architectural analysis and performance optimization.

  * Provide a new optimistic DAG execution engine for data analysis
  * Build a new columnar shredded in-memory format and execution model that minimizes data serialization/deserialization costs and operator complexity
  * Provide a model for runtime generated functions and relational operators that minimizes complexity and maximizes performance
  * Support queries against columnar on disk format (Parquet) and JSON
  * Support the most common set of standard SQL read-only phrases using ANSI standards. Includes: SELECT, FROM, WHERE, HAVING, ORDER, GROUP BY, IN, DISTINCT, LEFT JOIN, RIGHT JOIN, INNER JOIN
  * Support schema-on-read querying and execution
  * Build a set of columnar operation primitives including Merge Join, Sort, Streaming Aggregate, Filter, Selection Vector removal.
  * Support unlimited level of subqueries and correlated subqueries
  * Provided an extensible query-language agnostic JSON-base logical data flow syntax.
  * Support complex data type manipulation via logical plan operations

## Known Issues

SQL Parsing  
Because Apache Drill is built to support late-bound changing schemas while SQL
is statically typed, there are couple of special requirements that are
required writing SQL queries. These are limited to the current release and
will be correct in a future milestone release.

  * All tables are exposed as a single map field that contains
  * Drill Alpha doesn't support implicit or explicit casts outside those required above.
  * Drill Alpha does not include, there are currently a couple of differences for how to write a query in order to query against UDFs
  * Drill currently supports simple and aggregate functions using scalar, repeated and
  * Nested data support incomplete. Drill Alpha supports nested data structures as well repeated fields. 



