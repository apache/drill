---
title: "FROM Clause"
date: 2018-12-08
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

       tableReference:
       
       with_subquery_table_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
          | table_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
          | ( subquery ) [ AS ] alias [ ( column_alias [, ...] ) ]
          | <join_clause>
          | [ LATERAL ] [<lateral_join_type>] <lateral_subquery> [ON TRUE]
       
       join_clause:
          	tableReference <join_type> tableReference [ON <join_condition>]
       
       lateral_subquery:
             <unnest_table_expr>
          | ( SELECT_clause FROM <unnest_table_expr> [,...] )
       
       lateral_join_type:
       [INNER] JOIN
       LEFT [OUTER] JOIN
       
       unnest_table_expr:
        UNNEST '(' expression ')'  [AS] <alias_table_name>(<alias_column_name>)


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


   * *join_clause*  
     Identifies the tables with the data you want to join, the type of join to be performed on the tables, and the conditions on which to join the tables. Starting in Drill 1.14, Drill supports lateral joins. 

       **NOTE:** See [Lateral Join]({{site.baseurl}}/docs/lateral-join/) for additional information and examples of queries with lateral joins.  
  
   * *LATERAL*  
     Keyword that represents a lateral join. A lateral join is essentially a foreach loop in SQL. A lateral join combines the results of the outer query with the results of a lateral subquery. When you use the UNNEST relational operator, Drill infers the LATERAL keyword. 

   * *lateral\_sub_query*  
     A lateral subquery is like correlated subqueries except that you use a lateral subquery in the FROM clause instead of the WHERE clause. Also, lateral subqueries can return any number of rows; correlated subqueries return exactly one row.

   * *unnest\_table_expr*  
     References the table produced by the UNNEST relational operator. UNNEST converts a collection to a relation. You must use the UNNEST relational operator with LATERAL subqueries when a field contains repeated types, like an array of maps. You must also indicate an alias for the table produced by UNNEST. 

   * *lateral\_join_type*  
     The type of join used with the lateral subquery. Lateral subqueries support [INNER] JOIN and LEFT [OUTER] JOIN, for example:  

        ...FROM table1 LEFT OUTER JOIN LATERAL (select a from t2) ON TRUE;  
 
     If you do not indicate the join type, Drill infers an INNER JOIN.  

   * *ON TRUE*  
     The join condition when the results of a lateral subquery are joined with fields in rows of the table referenced. This condition is implicit. You do not have to include the condition in the query.  

   * *join_type*  
   Specifies the type of join between two tables in the join clause when you do not use a lateral join. The join clause supports the following join types:  
  
        [INNER] JOIN  
        LEFT [OUTER] JOIN  
        RIGHT [OUTER] JOIN  
        FULL [OUTER] JOIN

   * *ON join_condition*  
   A type of join specification where the joining columns are stated as a condition that follows the ON keyword, for example:  
  
        homes join listing on homes.listid=listing.listid and homes.homeid=listing.homeid

## Join Types
INNER JOIN  
Return matching rows only, based on the join condition or list of joining columns.  

OUTER JOIN  
Return all of the rows that the equivalent inner join would return plus non-matching rows from the "left" table, "right" table, or both tables. The left table is the first-listed table, and the right table is the second-listed table. The non-matching rows contain NULL values to fill the gaps in the output columns.  

LATERAL  
A lateral join is essentially a foreach loop in SQL. A lateral join is represented by the keyword LATERAL with an inner subquery in the FROM clause. See [Lateral Join]({{site.baseurl}}/docs/lateral-join/).  

CROSS JOIN  
Starting in Drill 1.15, Drill supports cross joins. A cross join returns the cartesian product of two tables. Cross joins are disabled by default because they can produce extremely large result sets that cause out of memory errors. 

To enable cross joins, disable the `planner.enable_nljoin_for_scalar_only` option, as shown:  

	set `planner.enable_nljoin_for_scalar_only` = false;  
	+-------+-------------------------------------------------+
	|  ok   |                 	summary                 	  | 
	+-------+-------------------------------------------------+
	| true  | planner.enable_nljoin_for_scalar_only updated.  |
	+-------+-------------------------------------------------+  

Before you enable the cross join functionality, verify that Drill has enough memory to process the query. Also note the following limitation related to the use of aggregate functions with cross joins.

**Limitation**  
If the input row count for an aggregate function is larger than the value set for the `planner.slice_target` option, Drill cannot plan the query. As a workaround, set the `planner.enable_multiphase_agg` option to false. This limitation will be resolved with [DRILL-6839](https://issues.apache.org/jira/browse/DRILL-6839).



## Usage Notes  
   * Joined columns must have comparable data types.
   * A join with the ON syntax retains both joining columns in its intermediate result set.  

## Examples   

The following example joins two tables on the table id: 

       SELECT tbl1.id, tbl1.type 
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
  

In the following example, assume you have the following two tables that you want to join using a cross join:

**Note:** These tables were created from the region.parquet and nation.parquet files in the sample-data folder included with the Drill installation.

	SELECT * FROM tmp.`n_name`;
	+----------+-----------------------+
	|  R_NAME  |       R_COMMENT   	|
	+----------+-----------------------+
	| AFRICA   | lar deposits. blithe  |
	| AMERICA  | hs use ironic, even   |
	| ASIA 	| ges. thinly even pin  |
	+----------+-----------------------+
	 
	SELECT * FROM tmp.`n_key`;
	+---------+--------------+
	| N_NAME  | N_NATIONKEY  |
	+---------+--------------+
	| 0   	  | ALGERIA  	 |
	| 1   	  | ARGENTINA	 |
	| 2   	  | BRAZIL   	 |
	+---------+--------------+
 
Using CROSS JOIN to join the two tables produces the following results:
 
	SELECT * FROM tmp.`n_key` CROSS JOIN tmp.`n_name`;
	+---------+--------------+----------+----------------------------+
	| N_NAME  | N_NATIONKEY  |  R_NAME  |   	R_COMMENT   	     |
	+---------+--------------+----------+----------------------------+
	| 0   	  | ALGERIA  	 | AFRICA   | lar deposits. blithe       |
	| 0   	  | ALGERIA  	 | AMERICA  | hs use ironic, even        |
	| 0   	  | ALGERIA  	 | ASIA     | ges. thinly even pin       |
	| 1   	  | ARGENTINA	 | AFRICA   | lar deposits. blithe       |
	| 1   	  | ARGENTINA	 | AMERICA  | hs use ironic, even        |
	| 1   	  | ARGENTINA	 | ASIA     | ges. thinly even pin       |
	| 2   	  | BRAZIL   	 | AFRICA   | lar deposits. blithe       |
	| 2   	  | BRAZIL   	 | AMERICA  | hs use ironic, even        |
	| 2   	  | BRAZIL   	 | ASIA 	| ges. thinly even pin       |
	+---------+--------------+----------+----------------------------+
	  



       

