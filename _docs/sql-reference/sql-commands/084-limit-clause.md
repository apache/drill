---
title: "LIMIT Clause"
date: 2018-08-02 22:29:54 UTC
parent: "SQL Commands"
---
The LIMIT clause limits the result set to the specified number of rows. You can use LIMIT with or without an ORDER BY clause.


## Syntax
The LIMIT clause supports the following syntax:  

       LIMIT { count | ALL }

Specifying ALL returns all records, which is equivalent to omitting the LIMIT clause from the SELECT statement.

## Parameters
*count*  
Specifies the maximum number of rows to return.
If the count expression evaluates to NULL, Drill treats it as LIMIT ALL.  

## LIMIT 0

LIMIT 0 quickly returns an empty set. Use LIMIT 0 to test the validity of the SQL syntax in a complex query or verify that the query is optimized before running the query on large data sets.  

### Usage Notes  


- Include the LIMIT 0 clause in the outermost query. Drill does not optimize LIMIT 0 queries if you include LIMIT 0 in a subquery.  
- When you run a query with the LIMIT 0 clause, Drill does not return any rows from the result set.  
- Run LIMIT 0 with a query and then evaluate the query plan to verify that the query is optimized.  
  

### LIMIT 0 Optimization  

Drill optimizes LIMIT 0 queries through the following options, which are enabled by default:  

**planner.enable\_limit0_optimization**  
Enables the query planner to determine data types returned by a query during the planning phase (before scanning data). Since Drill is a schema-free engine, the query planner does not know the column types of the result set before reading records. However, the query planner can infer the column types of the result set during query validation if you provide enough type information to Drill about the input column types. If the planner can infer all column types, a short execution path is planned instead of a full parallel execution.  

To provide column type information, you can:  

- Issue queries on Hive tables. The Hive metastore provides the input types.  
- Issue queries with explicit casts on table columns in the query, for example:  
  `SELECT CAST(col1 AS BIGINT), ABS(CAST(col2 AS DATE)) FROM table LIMIT 0;`  
- Issue queries on views with casts on table columns.  

**planner.enable\_limit0\_on_scan**  
Supported in Drill 1.14 and later. Enables Drill to determine data types as Drill scans data. This optimization is used when the query planner cannot infer types of columns during validation (prior to scanning). Drill exits and terminates the query immediately after resolving the types, and the query execution is not parallelized. When this optimization is applied, the query plan contains a LIMIT (0) above every SCAN, with an optional PROJECT in between.  

### LIMIT 0 Limitations  
The following sections list the types, operators, and functions that LIMIT 0 optimizations do not support.  

**Unsupported Types**  
LIMIT 0 optimizations do not apply to the following types:  

REAL, BINARY, VARBINARY, NULL, ANY, SYMBOL, MULTISET, ARRAY, MAP, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST 

**NOTE:** The query planner in Drill has other types that are not optimized because they are undefined in Drill’s type system.  

**Unsupported Hive UDF Types**  
Hive has a list of functions with return type NVARCHAR, which the query planner in Drill does not support; therefore, type inference does not work for the following functions:  


- BIN  
- CONCAT_WS  
- HEX  
- REFLECT  
- UNHEX  
- XPATH_STRING  

**Unsupported Operators and Functions**  
LIMIT 0 optimizations do not work for queries with the UNION [ALL] set operator or the following complex functions:  


- KVGEN or MAPPIFY  
- FLATTEN  
- CONVERT_FROMJSON  
- CONVERT_TOJSON  
- CONVERT_TOSIMPLEJSON  
- CONVERT_TOEXTENDEDJSON  
- AVG (window function)


 
   

 

 
 

## Examples
The following example query includes the ORDER BY and LIMIT clauses and returns the top 20 sales totals by month and state:  

       0: jdbc:drill:> SELECT `month`, state, SUM(order_total)
       AS sales FROM orders GROUP BY `month`, state
       ORDER BY 3 DESC LIMIT 20;
       +------------+------------+------------+
       |   month    |   state    |   sales    |
       +------------+------------+------------+
       | May        | ca         | 119586     |
       | June       | ca         | 116322     |
       | April      | ca         | 101363     |
       | March      | ca         | 99540      |
       | July       | ca         | 90285      |
       | October    | ca         | 80090      |
       | June       | tx         | 78363      |
       | May        | tx         | 77247      |
       | March      | tx         | 73815      |
       | August     | ca         | 71255      |
       | April      | tx         | 68385      |
       | July       | tx         | 63858      |
       | February   | ca         | 63527      |
       | June       | fl         | 62199      |
       | June       | ny         | 62052      |
       | May        | fl         | 61651      |
       | May        | ny         | 59369      |
       | October    | tx         | 55076      |
       | March      | fl         | 54867      |
       | March      | ny         | 52101      |
       +------------+------------+------------+
       20 rows selected

