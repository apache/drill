---
title: "SQL Reference Introduction"
date: 2018-11-02
parent: "SQL Reference"
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

