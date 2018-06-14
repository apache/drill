---
title: "Parquet Filter Pushdown"
date: 2018-06-14 22:23:22 UTC
parent: "Performance Tuning"
---

Drill 1.9 introduces the Parquet filter pushdown option. Parquet filter pushdown is a performance optimization that prunes extraneous data from a Parquet file to reduce the amount of data that Drill scans and reads when a query on a Parquet file contains a filter expression. Pruning data reduces the I/O, CPU, and network overhead to optimize Drill’s performance.
 
Parquet filter pushdown is enabled by default. When a query contains a filter expression, you can run the [EXPLAIN PLAN command]({{site.baseurl}}/docs/explain-commands/) to see if Drill applies Parquet filter pushdown to the query. You can enable and disable this feature using the [ALTER SYSTEM|SESSION SET]({{site.baseurl}}/docs/alter-system/) command with the `planner.store.parquet.rowgroup.filter.pushdown` option.  

As of Drill 1.13, the query planner in Drill can apply project push down, filter push down, and partition pruning to star queries in common table expressions (CTEs), views, and subqueries, for example:  
  
       select col1 from (select * from t)  

When a CTE, view, or subquery contains a star filter condition, the query planner in Drill can apply the filter and prune extraneous data, further reducing the amount of data that the scanner reads and improving performance. 
 
**Note:** Currently, Drill only supports pushdown for simple star subselect queries without filters. See [DRILL-6219](https://www.google.com/url?q=https://issues.apache.org/jira/browse/DRILL-6219&sa=D&ust=1522084453671000&usg=AFQjCNFXp-nWMRXzM466BSRFlV3F63_ZYA) for more information.  

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
Because Drill applies Parquet filter pushdown during the query planning phase, you can view the query execution plan to see if Drill pushes down the filter when a query on a Parquet file contains a filter expression. You can run the [EXPLAIN PLAN command]({{site.baseurl}}/docs/explain-commands/) to see the execution plan for the query, as shown in the following example.

**Example**
Starting in Drill 1.14, Drill supports the planner rule, JoinPushTransitivePredicatesRule, which enables Drill to infer filter conditions for join queries and push the filter conditions down to the data source. 

This example shows a query plan where the JoinPushTransitivePredicatesRule is used to push the filter down to each table referenced in the following query:  

       SELECT * FROM dfs.`/tmp/first` t1 JOIN dfs.`/tmp/second` t2  ON t1.`month` = t2.`month` WHERE t2.`month` = 4  

This query performs a join on two tables partitioned by the “month” column. The “first” table has 16 Parquet files, and the “second” table has 7. Issuing the `EXPLAIN PLAN FOR` command, you can see that the query planner applies the filter to both tables, significantly reducing the number of files read by the Scan operator in each table.  

       EXPLAIN PLAN FOR SELECT * FROM dfs.`/tmp/first` t1 JOIN dfs.`/tmp/second` t2  ON t1.`month` = t2.`month` WHERE t2.`month` = 4  
       
       DrillProjectRel(**=[$0], **0=[$2])
         DrillJoinRel(condition=[=($1, $3)], joinType=[inner])
           DrillScanRel(table=[[dfs, first]], groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=/tmp/first/0_0_9.parquet], ReadEntryWithPath [path=/tmp/first/0_0_10.parquet]], selectionRoot=file:/tmp/first, numFiles=2, numRowGroups=2, usedMetadataFile=false, columns=[`**`]]])
           DrillScanRel(table=[[dfs, second]], groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=/tmp/second/0_0_5.parquet]], selectionRoot=file:/tmp/second, numFiles=1, numRowGroups=1, usedMetadataFile=false, columns=[`**`]]])

 See [Query Plans]({{site.baseurl}}/docs/query-plans/) for more information. 

##Support 
The following table lists the supported and unsupported clauses, operators, data types, function, and scenarios for Parquet filter pushdown:  

**Note:** ^1^ indicates support as of Drill 1.13. ^2^ indicates support as of Drill 1.14.  

|                      | Supported                                                                                                                                                                                                                                                     | Not Supported                           |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| Clauses              | WHERE,   ^1^WITH, HAVING (HAVING is supported if Drill can pass the filter through GROUP   BY.)                                                                                                                                                                 | -                                       |
| Operators            | ^2^BETWEEN,   ^2^ITEM, AND, OR, NOT, ^1^IS [NOT] NULL, ^1^IS [NOT] TRUE, ^1^IS [NOT] FALSE, IN (An   IN list is converted to OR if the number in the IN list is within a certain   threshold, for example 20. If greater than the threshold, pruning cannot   occur.) | -                                       |
| Comparison Operators | <>,   <, >, <=, >=, =                                                                                                                                                                                                                                         | -                                       |
| Data Types           | INT,   BIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP, TIME, *BOOLEAN (true, false)                                                                                                                                                                                   | CHAR,   VARCHAR columns, Hive TIMESTAMP |
| Function             | CAST   is supported among the following types only: int, bigint, float, double,   ^1^date, ^1^timestamp, and ^1^time                                                                                                                                                | -                                       |
| Other                | ^2^Enabled   ^2^native Hive reader, Files with multiple row groups, Joins                                                                                                                                                                                       | -                                       |

**Note:** Drill cannot infer filter conditions for join queries that have: 

- a dynamic star in the sub-query or queries that include the WITH statement.  
- several filter predicates with the OR logical operator.  
- more than one EXISTS operator (instead of JOIN operators).  
- INNER JOIN and local filtering with a several conditions.                                                                                                    |   