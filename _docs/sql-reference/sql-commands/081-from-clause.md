---
title: "FROM Clause"
date: TBD 
parent: "SQL Commands"
---
The FROM clause lists the references (tables, views, and subqueries) that data is selected from. Drill expands the traditional concept of a “table reference” in a standard SQL FROM clause to refer to files and directories in a local or distributed file system.

## Syntax
The FROM clause supports the following syntax:

       ... FROM table_expression [, …]

## Parameters
*table_expression* 

Includes one or more *table_references* and is typically followed by the WHERE, GROUP BY, ORDER BY, or HAVING clause. 

*table_reference*

       with_subquery_table_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
       table_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
       ( subquery ) [ AS ] alias [ ( column_alias [, ...] ) ]
       table_reference [ ON join_condition ]

   * *with\_subquery\_table_name*  
   A table defined by a subquery in the WITH clause.

  * *table_name*  
  Name of a table or view. In Drill, you can also refer to a file system directory or a specific file.

   * *alias*  
   A temporary alternative name for a table or view that provides a convenient shortcut for identifying tables in other parts of a query, such as the WHERE clause. You must supply an alias for a table derived from a subquery. Aliases might be required for [querying nested JSON]({{site.baseurl}}/docs/json-data-model/#analyzing-json). Aliases are definitely required to resolve ambiguous references, such as using the name "user" to query the Drill profiles. Drill treats "user" as a function in this case, and the returns unexpected results. If you use a table alias, Drill treats "user" as a column identifier, and the query returns expected results. The AS keyword is always optional. Drill does not support the GROUP BY alias.

   * *column_alias*  
   A temporary alternative name for a column in a table or view. You can use named column aliases in the SELECT list to provide meaningful names for regular columns and computed columns, such as the results of aggregate functions. You cannot reference column aliases in the following clauses:  
       WHERE  
       GROUP BY  
       HAVING  

       Because Drill works with schema-less data sources, you cannot use positional aliases (1, 2, etc.) to refer to SELECT list columns, except in the ORDER BY clause.

   * *subquery*  
   A query expression that evaluates to a table. The table exists only for the duration of the query and is typically given a name or alias, though an alias is not required. You can also define column names for tables that derive from subqueries. Naming column aliases is important when you want to join the results of subqueries to other tables and when you want to select or constrain those columns elsewhere in the query. A subquery may contain an ORDER BY clause, but this clause may have no effect if a LIMIT or OFFSET clause is not also specified. You can use the following subquery operators in Drill queries. These operators all return Boolean results.  
       * ALL  
       * ANY  
       * EXISTS  
       * IN  
       * SOME  
      
       In general, correlated subqueries are supported. EXISTS and NOT EXISTS subqueries that do not contain a correlation join are not yet supported.

   * *join_type*  
   Specifies one of the following join types:  
       [INNER] JOIN  
       LEFT [OUTER] JOIN  
       RIGHT [OUTER] JOIN  
       FULL [OUTER] JOIN

   * *ON join_condition*  
   A type of join specification where the joining columns are stated as a condition that follows the ON keyword.  
       Example:  ` homes join listing on homes.listid=listing.listid and homes.homeid=listing.homeid`

## Join Types
INNER JOIN  
Return matching rows only, based on the join condition or list of joining columns.  

OUTER JOIN  
Return all of the rows that the equivalent inner join would return plus non-matching rows from the "left" table, "right" table, or both tables. The left table is the first-listed table, and the right table is the second-listed table. The non-matching rows contain NULL values to fill the gaps in the output columns.

## Usage Notes  
   * Joined columns must have comparable data types.
   * A join with the ON syntax retains both joining columns in its intermediate result set.  

## Example  

       0: jdbc:drill:zk=local> SELECT tbl1.id, tbl1.type 
       FROM dfs.`/Users/brumsby/drill/donuts.json` 
       AS tbl1
       JOIN
       dfs.`/Users/brumsby/drill/moredonuts.json` as tbl2
       ON tbl1.id=tbl2.id;
       
       +------------+------------+
       |     id     |    type    |
       +------------+------------+
       | 0001       | donut      |
       +------------+------------+
       
       1 row selected (0.395 seconds)
       

