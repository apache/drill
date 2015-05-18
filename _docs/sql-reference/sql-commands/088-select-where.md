---
title: "SELECT WHERE"
parent: "SQL Commands"
---
The WHERE clause selects rows based on a boolean expression. Only rows for which the expression evaluates to TRUE are returned in the result.

## Syntax
The WHERE clause supports the following syntax:

       [ WHERE boolean_expression ]  

## Expression  
A boolean expression can include one or more of the following operators:  

  * AND
  * OR
  * NOT
  * IS NULL
  * IS NOT NULL
  * LIKE 
  * BETWEEN
  * IN
  * EXISTS
  * Comparison operators
  * Quantified comparison operators


## Examples
The following query compares order totals where the states are California and New York:  

       0: jdbc:drill:> select o1.cust_id, sum(o1.order_total) as ny_sales,
       (select sum(o2.order_total) from hive.orders o2
       where o1.cust_id=o2.cust_id and state='ca') as ca_sales
       from hive.orders o1 where o1.state='ny' group by o1.cust_id
       order by cust_id limit 20;
       +------------+------------+------------+
       |  cust_id   |  ny_sales  |  ca_sales  |
       +------------+------------+------------+
       | 1001       | 72         | 47         |
       | 1002       | 108        | 198        |
       | 1003       | 83         | null       |
       | 1004       | 86         | 210        |
       | 1005       | 168        | 153        |
       | 1006       | 29         | 326        |
       | 1008       | 105        | 168        |
       | 1009       | 443        | 127        |
       | 1010       | 75         | 18         |
       | 1012       | 110        | null       |
       | 1013       | 19         | null       |
       | 1014       | 106        | 162        |
       | 1015       | 220        | 153        |
       | 1016       | 85         | 159        |
       | 1017       | 82         | 56         |
       | 1019       | 37         | 196        |
       | 1020       | 193        | 165        |
       | 1022       | 124        | null       |
       | 1023       | 166        | 149        |
       | 1024       | 233        | null       |
       +------------+------------+------------+
