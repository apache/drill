---
title: "SQL Reference"
---
Drill supports the ANSI standard for SQL. You can use SQL to query your Hive,
HBase, and distributed file system data sources. Drill can discover the form
of the data when you submit a query. You can query text files and nested data
formats, such as JSON and Parquet. Drill provides special operators and
functions that you can use to drill down into nested data formats.

Drill queries do not require information about the data that you are trying to
access, regardless of its source system or its schema and data types. The
sweet spot for Apache Drill is a SQL query workload against *complex data*:
data made up of various types of records and fields, rather than data in a
recognizable relational form (discrete rows and columns).

For more information about SQL for Drill, see:

- [Data Types](http://drill.apache.org/docs/data-types/)
- [Lexical Structure](http://drill.apache.org/docs/lexical-structure/)
- [Operators](http://drill.apache.org/docs/operators/)
- [SQL Functions](http://drill.apache.org/docs/sql-functions/)
- [Nested Data Functions](http://drill.apache.org/docs/nested-data-functions/)
- [SQL Commands Summary](http://drill.apache.org/docs/sql-commands-summary/)
- [Reserved Key Words](http://drill.apache.org/docs/reserved-keywords/)
- [SQL Extensions](http://drill.apache.org/docs/sql-extensions/)

