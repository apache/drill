---
title: "HAVING Clause"
date: 2018-11-02
parent: "SQL Commands"
---
The HAVING clause filters group rows created by the GROUP BY clause. The HAVING clause is applied to each group of the grouped table, much as a WHERE clause is applied to a select list. If there is no GROUP BY clause, the HAVING clause is applied to the entire result as a single group. The SELECT clause cannot refer directly to any column that does not have a GROUP BY clause.

## Syntax
The HAVING clause supports the following syntax:  

       HAVING  boolean_expression 

## Expression  
A *boolean expression* can include one or more of the following operators:  

  * AND
  * OR
  * NOT
  * IS NULL
  * IS NOT NULL
  * LIKE 
  * BETWEEN
  * IN
  * Comparison operators
  * Quantified comparison operators  

## Usage Notes
  * Any column referenced in a HAVING clause must be either a grouping column or a column that refers to the result of an aggregate function.
  * In a HAVING clause, you cannot specify:
   * An alias that was defined in the select list. You must repeat the original, unaliased expression. 
   * An ordinal number that refers to a select list item. Only the GROUP BY and ORDER BY clauses accept ordinal numbers.

## Examples
The following example query uses the HAVING clause to constrain an aggregate result. Drill queries the `dfs.clicks workspace` and  returns the total number of clicks for devices that indicate high click-throughs:  

       0: jdbc:drill:> select t.user_info.device, count(*) from \`clicks/clicks.json\` t 
       group by t.user_info.device having count(*) > 1000;  
       
       +------------+------------+
       |   EXPR$0   |   EXPR$1   |
       +------------+------------+
       | IOS5       | 11814      |
       | AOS4.2     | 5986       |
       | IOS6       | 4464       |
       | IOS7       | 3135       |
       | AOS4.4     | 1562       |
       | AOS4.3     | 3039       |
       +------------+------------+  

The aggregate is a count of the records for each different mobile device in the clickstream data. Only the activity for the devices that registered more than 1000 transactions qualify for the result set.


