---
title: "ORDER BY Clause"
date: 2018-11-02
parent: "SQL Commands"
---
The ORDER BY clause sorts the result set of a query.



## Syntax
The ORDER BY clause supports the following syntax:

       [ ORDER BY expression
       [ ASC | DESC ]
       [ NULLS FIRST | NULLS LAST ]

  

## Parameters  
*expression*  

Defines the sort order of the query result set, typically by specifying one or more columns in the select list.  

You can also specify:  

   * Columns that are not in the select list 
   * Expressions formed from one or more columns that exist in the tables referenced by the query
   * Ordinal numbers that represent the position of select list entries (or the position of columns in the table if no select list exists)
   * Aliases that define select list entries
   
When the ORDER BY clause contains multiple expressions, the result set is sorted according to the first expression, then the second expression is applied to rows that have matching values from the first expression, and so on.

ASC  
Specifies that the results should be returned in ascending order. If the order is not specified, ASC is the default.

DESC  
Specifies that the results should be returned in descending order. 

NULLS FIRST  
Specifies that NULL values should be returned before non-NULL values.  

NULLS LAST  
Specifies that NULL values should be returned after non-NULL values.

## Usage Notes
   * NULL values are considered "higher" than all other values. With default ascending sort order, NULL values sort at the end.  
   * When a query does not contain an ORDER BY clause, the system returns result sets with no predictable ordering of the rows. The same query executed twice might return the result set in a different order.  
   * In any parallel system, when ORDER BY does not produce a unique ordering, the order of the rows is non-deterministic. That is, if the ORDER BY expression produces duplicate values, the return order of those rows may vary from other systems or from one run the system to the next.

## Examples
The following example query returns sales totals for each month in descending order, listing the highest sales month to the lowest sales month:

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




