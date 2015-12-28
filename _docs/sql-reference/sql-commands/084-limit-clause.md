---
title: "LIMIT Clause"
date: 
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

