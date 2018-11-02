---
title: "SELECT List"
date: 2018-11-02
parent: "SQL Commands"
---

The SELECT list names the columns, functions, and expressions that you want the query to return. The list represents the output of the query.

## Syntax  

The SELECT list supports the following syntax:  

       SELECT  [ DISTINCT ] COLUMNS[n] | * | expression [ AS column_alias ] [, ...]  

## Parameters
COLUMNS[*n*]  
Array columns are used for reading data from text files. Use the columns[*n*] syntax in the SELECT list to return rows from text files in a columnar format. This syntax uses a zero-based index, so the first column is column 0.  

DISTINCT  
An option that eliminates duplicate rows from the result set, based on matching values in one or more columns.
* (asterisk)
Returns the entire contents of the table or file.

*expression*  
An expression formed from one or more columns that exist in the tables, files, or directories referenced by the query. An expression can contain functions and aliases that define select list entries. You can also use a scalar aggregate subquery as the expression in the SELECT list. 

*scalar aggregate subquery*  
A scalar aggregate subquery is a regular SELECT query in parentheses that returns exactly one column value from one row. The returned value is used in the outer query. The scalar aggregate subquery must include an aggregate function, such as MAX(), AVG(), or COUNT(). If the subquery returns zero rows, the value of the subquery expression is null. If it returns more than one row, Drill returns an error.  Scalar subqueries are not valid expressions in the following cases:  

* As default values for expressions
* In GROUP BY and HAVING clauses  

AS *column_alias*  
A temporary name for a column in the final result set. The AS keyword is optional.  

## Examples
The following example shows a query with a scalar subquery expression:  

       SELECT a1, (SELECT MAX(a2) FROM t2) AS max_a2 FROM t1;       
       +-----+-------+
       | a1   | max_a2 |
       +-----+-------+
       | 1    | 9 |
       | 2    | 9 |
       | 3    | 9 |
       | 4    | 9 |
       | 5    | 9 |
       | 6    | 9 |
       | 7    | 9 |
       | null | 9 |
       | 9    | 9 |
       | 10   | 9 |
       +-----+-------+
       10 rows selected (0.244 seconds)

The following example shows a query with array columns that return rows in column format for easier readability:  

       SELECT COLUMNS[0], COLUMNS[1] FROM dfs.`/Users/brumsby/drill/plays.csv`;       
       +------------+------------------------+
       |   EXPR$0   |         EXPR$1         |
       +------------+------------------------+
       | 1599       | As You Like It         |
       | 1601       | Twelfth Night          |
       | 1594       | Comedy of Errors       |
       | 1595       | Romeo and Juliet       |
       | 1596       | The Merchant of Venice |
       | 1610       | The Tempest            |
       | 1599       | Hamlet                 |
       +------------+------------------------+
       7 rows selected (0.137 seconds)
       

