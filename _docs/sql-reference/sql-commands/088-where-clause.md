---
title: "WHERE Clause"
date: 2018-11-02
parent: "SQL Commands"
---
The WHERE clause selects rows based on a boolean expression. Only rows for which the expression evaluates to TRUE are returned in the result.

## Syntax
The WHERE clause supports the following syntax:

       WHERE boolean_expression  

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

       0: jdbc:drill:> SELECT o1.cust_id, sum(o1.order_total) AS ny_sales,
       (SELECT SUM(o2.order_total) FROM hive.orders o2
       WHERE o1.cust_id=o2.cust_id and state='ca') AS ca_sales
       FROM hive.orders o1 WHERE o1.state='ny' GROUP BY o1.cust_id
       ORDER BY cust_id LIMIT 20;
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

The following query uses a workspace named `dfw.views` and joins a view named “custview” with a hive table named “orders” to determine sales for each membership type:

       0: jdbc:drill:> select membership, sum(order_total) as sales from hive.orders, custview
       where orders.cust_id=custview.cust_id
       group by membership order by 2;
       +------------+------------+
       | membership |   sales    |
       +------------+------------+
       | "basic"    | 380665     |
       | "silver"   | 708438     |
       | "gold"     | 2787682    |
       +------------+------------+
       3 rows selected
