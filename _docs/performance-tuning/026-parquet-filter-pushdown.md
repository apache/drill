---
title: "Parquet Filter Pushdown"
date: 2016-11-21 21:25:59 UTC
parent: "Performance Tuning"
---

Drill 1.9 introduces the Parquet filter pushdown option. Parquet filter pushdown is a performance optimization that prunes extraneous data from a Parquet file to reduce the amount of data that Drill scans and reads when a query on a Parquet file contains a filter expression. Pruning data reduces the I/O, CPU, and network overhead to optimize Drill’s performance.
 
Parquet filter pushdown is enabled by default. When a query contains a filter expression, you can run the [EXPLAIN PLAN command]({{site.baseurl}}/docs/explain-commands/) to see if Drill applies Parquet filter pushdown to the query. You can enable and disable this feature using the [ALTER SYSTEM|SESSION SET]({{site.baseurl}}/docs/alter-system/) command with the `planner.store.parquet.rowgroup.filter.pushdown` option.  

##How Parquet Filter Pushdown Works
Drill applies Parquet filter pushdown during the query planning phase. The query planner in Drill performs Parquet filter pushdown by evaluating the filter expressions in the query. If no filter expression exists, the underlying scan operator reads all of the data in a Parquet file and then sends the data to operators downstream. When filter expressions exist, the planner applies each filter and prunes the data, reducing the amount of data that the scanner and Parquet reader must read.
 
Parquet filter pushdown is similar to partition pruning in that it reduces the amount of data that Drill must read during runtime. Parquet filter pushdown relies on the minimum and maximum value statistics in the row group metadata of the Parquet file to filter and prune data at the row group level. Drill can use any column in a filter expression as long the column in the Parquet file contains statistics. Whereas, partition pruning requires data to be partitioned on a column. A partition is created for each unique value in the column. Partition pruning can only prune data when the filter uses the partitioned column.  
 
The query planner looks at the minimum and maximum values in each row group for an intersection. If no intersection exists, the planner can prune the row group in the table. If the minimum and maximum value range is too large, Drill does not apply Parquet filter pushdown. The query planner can typically prune more data when the tables in the Parquet file are sorted by row groups.  

##Using Parquet Filter Pushdown
Currently, Parquet filter pushdown only supports filters that reference columns from a single table (local filters). Parquet filter pushdown requires the minimum and maximum values in the Parquet file metadata. All Parquet files created in Drill using the CTAS statement contain the necessary metadata. If your Parquet files were created using another tool, you may need to use Drill to read and rewrite the files using the [CTAS command]({{site.baseurl}}/docs/create-table-as-ctas-command/).
 
Parquet filter pushdown works best if you presort the data. You do not have to sort the entire data set at once. You can sort a subset of the data set, sort another subset, and so on. 

###Configuring Parquet Filter Pushdown  
Use the [ALTER SYSTEM|SESSION SET]({{site.baseurl}}/docs/alter-system/) command with the Parquet filter pushdown options to enable or disable the feature, and set the number of row groups for a table.  

The following table lists the Parquet filter pushdown options with their descriptions and default values:  

|       Option                                               | Description                                                                                                                                                                                                                                                                                                                                                | Default   |
|------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| "planner.store.parquet.rowgroup.filter.pushdown"           | Turns the Parquet filter pushdown feature on or   off.                                                                                                                                                                                                                                                                                                     | TRUE      |
| "planner.store.parquet.rowgroup.filter.pushdown.threshold" | Sets the number of row groups that a table can   have. You can increase the threshold if the filter can prune many row groups.   However, if this setting is too high, the filter evaluation overhead   increases. Base this setting on the data set. Reduce this setting if the   planning time is significant, or you do not see any benefit at runtime. | 10,000    |  

###Viewing the Query Plan
Because Drill applies Parquet filter pushdown during the query planning phase, you can view the query execution plan to see if Drill pushes down the filter when a query on a Parquet file contains a filter expression.
 
Run the [EXPLAIN PLAN command]({{site.baseurl}}/docs/explain-commands/) to see the execution plan for the query. See [Query Plans]({{site.baseurl}}/docs/query-plans/) for more information. 

##Support 
The following table lists the supported and unsupported clauses, operators, data types, and scenarios for Parquet filter pushdown:  

|                      | Supported                                                                                                                                                                          | Not Supported                                                                                            |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| Clauses              | WHERE,   HAVING (HAVING is supported if Drill can pass the filter through GROUP   BY.)                                                                                             |                                                                                                          |
| Operators            | AND,   OR, IN (An IN list is converted to OR if the number in the IN list is within   a certain threshold, for example 20. If greater than the threshold, pruning   cannot occur.) | NOT,   ITEM (Drill does not push the filter past the ITEM operator, which is used   for complex fields.) |
| Comparison Operators | <>,   <, >, <=, >=, =                                                                                                                 | IS [NOT] NULL                                                                                            |
| Data Types           | INT,   BIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP, TIME                                                                                                                                | CHAR,   VARCHAR columns, Hive TIMESTAMP                                                                  |
| Function             | CAST   is supported among these four numeric types only: int, bigint, float, double                                                                                                |                                                                                                          |
| Other                | --                                                                                                                                                                                 | Joins,   Files with multiple row groups, Enabled Native Hive reader                                      | 


