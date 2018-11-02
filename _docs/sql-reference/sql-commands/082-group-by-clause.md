---
title: "GROUP BY Clause"
date: 2018-11-02
parent: "SQL Commands"
---
The GROUP BY clause identifies the grouping columns for the query. You typically use a GROUP BY clause in conjunction with an aggregate expression. Grouping columns must be declared when the query computes aggregates with standard functions such as SUM, AVG, and COUNT. Currently, Drill does not support grouping on aliases.


## Syntax
The GROUP BY clause supports the following syntax:  


    GROUP BY expression [, ...]
  

## Parameters  
*column_name*  

Must be a column from the current scope of the query. For example, if a GROUP BY clause is in a subquery, it cannot refer to columns in the outer query.

*expression*  

The list of columns or expressions must match the list of non-aggregate expressions in the select list of the query.


## Usage Notes
*SelectItems* in the SELECT statement with a GROUP BY clause can only contain aggregates or grouping columns.


## Examples
The following query returns sales totals grouped by month:  

       0: jdbc:drill:> select `month`, sum(order_total)
       from orders group by `month` order by 2 desc;
       +------------+------------+
       | month | EXPR$1 |
       +------------+------------+
       | June | 950481 |
       | May | 947796 |
       | March | 836809 |
       | April | 807291 |
       | July | 757395 |
       | October | 676236 |
       | August | 572269 |
       | February | 532901 |
       | September | 373100 |
       | January | 346536 |
       +------------+------------+




