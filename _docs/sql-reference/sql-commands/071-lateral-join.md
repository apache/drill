---
title: "LATERAL Join"
date: 2018-08-06 23:12:05 UTC
parent: "SQL Commands"
---

**NOTE:** Drill 1.14 provides an early release of lateral join. The lateral join functionality is disabled by default because feature testing is in progress. If you want to experiment with lateral joins in your queries, enable the `planner.enable_unnest_lateral` option using the [SET]({{site.baseurl}}/docs/set/) command.  

A lateral join is essentially a foreach loop in SQL. A lateral join is represented by the keyword LATERAL with an inner subquery in the FROM clause, as shown in the following simple representation:
 
       SELECT <columns>
       FROM <tableReference>
       LATERAL <innerSubquery>;
 
Similar to a correlated subquery, a lateral inner subquery can refer to fields in rows of the table reference to determine which rows to return. A lateral subquery iterates through each row in the table reference, evaluating the inner subquery for each row, like a foreach loop. The rows returned by the inner subquery are added to the result of the join with the outer query. Without the LATERAL keyword, each subquery is evaluated independently and cannot refer to items in FROM.  

## Syntax 

    ...FROM tableReference  

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
                

## Parameters  

The following parameters are specific to lateral join. The list does not include all of the parameters applicable to the [FROM clause]({{site.baseurl}}/docs/from-clause/):  

* *join_clause*  
     Identifies the tables with the data you want to join, the type of join to be performed on the tables, and the conditions on which to join the tables. Starting in Drill 1.14, Drill supports lateral joins. 
  
  
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

## Usage Notes 

**LATERAL Subqueries**  


- Lateral subqueries are similar to correlated subqueries except that you use a lateral subquery in the FROM clause instead of the WHERE clause. Also, lateral subqueries can return any number of rows; correlated subqueries return exactly one row.  

- You can use lateral subqueries with the LEFT OUTER and INNER join types, for example:  
 
        ...FROM table1 LEFT OUTER JOIN LATERAL (select a from t2) ON true;  

- If you use the LATERAL keyword in a query, you must include the UNNEST relational operator in the subquery. However, if you use UNNEST without LATERAL, Drill infers the LATERAL keyword. 

- Use LATERAL subqueries with the UNNEST operator when a field contains repeated types, like an array of maps.  

- Lateral subqueries support the following operators: 

       - Limit  
       - Filter  
       - Project  
       - TopN  
       - Sort  
       - HashAgg  

**UNNEST Relational Operator**  

- A relational operator that behaves like a table function; UNNEST converts a collection to a relation.  
- UNNEST works on arrays; it creates a table with one field and multiple rows (one row for each entry in the array).  
- You can only use UNNEST in the FROM clause.  
- You cannot use UNNEST without LATERAL; if you use UNNEST without LATERAL, Drill infers a LATERAL join.  
- UNNEST can only unnest one column. However, the array in the column can be a multi-nested array.   
- Unnests data in small chunks, which typically provides a performance advantage when applying filters or running subqueries.  
- Unnest operators on each row of the source table; data can be partitioned without having to use a window function.  
- For queries with nested laterals, you must provide a name (alias) for the table that UNNEST generates.  
- UNNEST and LATERAL work similarly to FLATTEN, but differ from FLATTEN in the follow ways:  
       - UNNEST is a SQL standard, whereas FLATTEN is not.  
       - FLATTEN is only allowed in the SELECT list of query, not in the FROM clause.  
       - FLATTEN does not work with schema changes, but UNNEST can if the queries do not have hash aggregates. FLATTEN requires all data in a column to be of the same type. For example, if a column contains integers in some rows and float in others, UNNEST can process the query, whereas FLATTEN cannot.    
       - LATERAL and UNNEST cover a wider set of use cases than FLATTEN. For example, when you use LATERAL and UNNEST, Drill can perform a LEFT OUTER JOIN on data. If you used FLATTEN, Drill must scan the source table twice to perform an OUTER JOIN after flattening the data. Also, with LATERAL and UNNEST, you can apply a filter, aggregate, or limit on each row. With FLATTEN, the filter or aggregate is applied after flattening, however you cannot apply the limit on each row.  
       - FLATTEN unnests data into a table and processes the entire table; filters and subqueries are applied on the entire table at the same time.  


## LATERAL Join Query Examples  

The following sections provide query examples to demonstrate the use of lateral joins.  

### Simplified Query using LATERAL and UNNEST  

The following customer table contains customer data, including customer orders and returns, with the order and return data stored as complex types (arrays of maps). The store_id column is a foreign key.  


<table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0
 style='border-collapse:collapse'>
 <tr style='height:.5in'>
  <td valign=top style='border:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;
  height:.5in'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>cust_id</span></p>
  </td>
  <td valign=top style='border:solid black 1.0pt;border-left:none;padding:5.0pt 5.0pt 5.0pt 5.0pt;
  height:.5in'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>cust_name</span></p>
  </td>
  <td valign=top style='border:solid black 1.0pt;border-left:none;padding:5.0pt 5.0pt 5.0pt 5.0pt;
  height:.5in'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>store_id<br>
   </span></p>
  </td>
  <td valign=top style='border:solid black 1.0pt;border-left:none;padding:5.0pt 5.0pt 5.0pt 5.0pt;
  height:.5in'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>orders</span></p>
  </td>
  <td valign=top style='border:solid black 1.0pt;border-left:none;padding:5.0pt 5.0pt 5.0pt 5.0pt;
  height:.5in'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>returns</span></p>
  </td>
 </tr>
 <tr style='height:135.0pt'>
  <td valign=top style='border:solid black 1.0pt;border-top:none;padding:5.0pt 5.0pt 5.0pt 5.0pt;
  height:135.0pt'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>101</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:135.0pt'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>Fred</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:135.0pt'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>5</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:135.0pt'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>[<br>
   &nbsp;&nbsp;{order_id: 1, order_date: 10/10/2017, order_amount: $200, items:<br>
   &nbsp;[{type: “chair”, quantity: 3}, {type: ...} ] },</span></p>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'> &nbsp;&nbsp;{order_id: 2,
  order_date:<br>
   &nbsp;11/10/2017, order_amount: $500, &nbsp;items: [{type: “lamp”, quantity:
  2},<br>
   &nbsp;{type: ...}]}</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:135.0pt'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'> [ {order_id: 2,
  return_date: 11/20/2017, return_amount: $200,<br>
   &nbsp;items: [{type: ...} ] } ]</span></p>
  </td>
 </tr>
 <tr style='height:.5in'>
  <td valign=top style='border:solid black 1.0pt;border-top:none;padding:5.0pt 5.0pt 5.0pt 5.0pt;
  height:.5in'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>102</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:.5in'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>Jack</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:.5in'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>7</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:.5in'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>&lt;orders<br>
   &nbsp;data&gt;</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:.5in'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>&lt;returns<br>
   &nbsp;data&gt;</span></p>
  </td>
 </tr>
 <tr style='height:23.0pt'>
  <td valign=top style='border:solid black 1.0pt;border-top:none;padding:5.0pt 5.0pt 5.0pt 5.0pt;
  height:23.0pt'></td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:23.0pt'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>...</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:23.0pt'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>...</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:23.0pt'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>...</span></p>
  </td>
  <td valign=top style='border-top:none;border-left:none;border-bottom:solid black 1.0pt;
  border-right:solid black 1.0pt;padding:5.0pt 5.0pt 5.0pt 5.0pt;height:23.0pt'>
  <p class=MsoNormal style='margin-bottom:0in;margin-bottom:.0001pt;line-height:
  normal'><span style='font-size:9.0pt;color:black'>...</span></p>
  </td>
 </tr>
</table>

If you want to query the Customer table for the average order amount for each customer in the month of November, you could write the following query, which uses a combination of a left outer join, group by, and the flatten function to unnest the orders field that contains complex types:  

       SELECT t1.cust_name, t2.avg_orders
       FROM  customer t1
       LEFT
       OUTER JOIN
       
          (SELECT f.cust_id,
       AVG(f.order_amount) as avg_orders
             FROM (SELECT
       cust_id, FLATTEN(customer.orders)  FROM customer) f
             WHERE
       f.order_date between date ‘11012017’
       and date ‘11302017’
              GROUP BY
       f.cust_id
       
          ) t2  on t1.cust_id =
       t2.cust_id  

However, this query is complex and inefficient. Writing the query with a lateral subquery resolves the inefficiencies:  

       SELECT cust_name, avg_orders
       FROM customer c
       LATERAL (SELECT AVG(order_amount) as avg_orders FROM UNNEST(c.orders)o
       WHERE o.order_date BETWEEN date ‘11012017’ and date ‘11302017’);  

Note that the FROM clause in the subquery references the ‘orders’ array from the table alias ‘c’ which is the outer table.  The reference to an outer table within the subquery makes this look like a correlated subquery.  However, there is an important difference in that a correlated subquery is used in the WHERE clause whereas what we really want is the set of rows from the un-nested array exposed as a ‘sub-table’ such that relevant filtering, aggregation, and so on can be performed.  

### Example Queries with LATERAL, UNNEST, and Aliases  

The following query examples demonstrate the use of the LATERAL keyword and UNNEST relational operator with aliases.  

**Example 1: One level lateral with an alias and implicit LATERAL**  
 
Drill infers a LATERAL join when you use the UNNEST relational operator. In the following query, UNNEST produces a table with one column that contains an unnested customer.c\_orders column. In this query, the table produced by unnest is aliased as _orders and the column is aliased as c\_order.

       SELECT 
         customer.c_name, customer.c_address, _orders.c_order.o_orderkey, _orders.c_order.o_totalprice
       FROM 
         dfs.`/Users/user1/work/data/complex/cust_order` customer, 
       UNNEST(customer.c_orders) _orders(c_order)
       ;  

**Example 2: One level lateral with an alias and explicit LATERAL**  
 
The following query contains the LATERAL keyword with a subquery that selects all the columns (SELECT *) from the table produced by the UNNEST relational operator. The table produced by UNNEST is aliased as _orders(c\_order). The subquery result is a table aliased as t\_orders. Drill performs the lateral join on the table that results from the subquery (t\_orders). The query produces the same query plan as the previous example (Example 1) where LATERAL was inferred.  

       SELECT 
         customer.c_name, customer.c_address, t_orders.c_order.o_orderkey, t_orders.c_order.o_totalprice
       FROM 
         dfs.`/Users/user1/work/data/complex/cust_order` customer, 
       LATERAL (
       SELECT * from UNNEST(customer.c_orders) _orders(c_order)
       ) t_orders
       ;  

**Example 3: Multi-level lateral with alias**  

A multi-level lateral is a nested lateral query. When a query contains nested laterals, you must use aliases for the tables generated by the UNNEST relational operator. If you do not include aliases, the query parser cannot discern between table and column names and returns parsing errors. 

In the following query, the first level UNNEST (line 8) that corresponds to the first level LATERAL (line 5) produces a table with a single column that is aliased as _order(c\_order). Since c\_order is a map, the select clause in the corresponding subquery (line 7) projects only the required fields. As a result, the subquery produces a table that is aliased as t\_orders(orderkey, orderamt, lineitems).

The table t\_orders is then processed by the second level LATERAL (line 10) and the corresponding second level UNNEST (line 13) will unnest the field t\_orders.lineitems. The second level lateral subquery produces a table aliased as t\_items. 

Note that the SELECT in the outer query can now refer to the tables t\_orders and t\_items.

        1: SELECT 
        2:   customer.c_name, customer.c_address, t_orders.orderkey, t_orders.orderamt, t_items.item.l_partkey, t_items.item.l_linenumber 
        3: FROM 
        4:   dfs.`/Users/pchandra/work/data/complex/cust_order` customer, 
        5: LATERAL
        6:    ( 
        7:      select _orders.c_order.o_orderkey orderkey, _orders.c_order.o_totalprice orderamt, _orders.c_order.o_lineitems as lineitems
        8:      FROM UNNEST(customer.c_orders) _orders(c_order)  
        9:    ) t_orders,
       10:    LATERAL
       11:    (
       12:      SELECT * 
       13:      FROM UNNEST(t_orders.lineitems) AS _items(item) 
       14:    ) t_items
       15: ;  

**Example 4: One level lateral with aggregation**  

For every customer, the following query returns the number of orders for each priority level.
The subquery that corresponds to the lateral, aggregates the order count, grouped by the priority, in the table _orders(c\_order) produced by UNNEST.

       SELECT 
         customer.c_name, customer.c_address, t_orders.o_orderCount, t_orders.o_orderpriority
       FROM 
         dfs.`/Users/user1/work/data/complex/cust_order` customer, 
         lateral (
          select count(_orders.c_order.o_orderkey) as o_orderCount, _orders.c_order.o_orderpriority as o_orderpriority from UNNEST(customer.c_orders) _orders(c_order) group by _orders.c_order.o_orderpriority
         ) t_orders
       ;  


### FLATTEN vs LATERAL and UNNEST Queries  

Each of the following examples shows a query written with the FLATTEN function and also written with a lateral join and UNNEST relational operator, which performs like a table function. These queries demonstrate how to use LATERAL and UNNEST instead of FLATTEN to simplify the writing of such queries.   

**Example 1** 

The queries in this example return the order total for the top 50 customers.  

*FLATTEN*  

       SELECT l.c_custkey, l.c_name, r.orderkey, 
       SUM(r.totalprice) sum_order 
       FROM customer l 
       INNER JOIN (SELECT g.custkey custkey, g.name, g.orderkey, g.totalprice totalprice 
       FROM (SELECT row_number() OVER(PARTITION BY c_custkey) 
       AS rn, f.c_custkey custkey, f.c_name name, f.o.o_orderkey orderkey, f.o.o_totalprice totalprice 
       FROM (SELECT c_custkey, c_name, 
       FLATTEN(c_orders) 
       AS o 
       FROM customer) f) g) r 
       ON (l.c_custkey = r.custkey) 
       GROUP BY l.c_custkey, l.c_name, r.orderkey 
       ORDER BY sum_order 
       LIMIT 50;  

*LATERAL and UNNEST*   

       SELECT customer.c_custkey, customer.c_name, orders.o_orderkey, sum(orders.o_totalprice) total_spending from customer, 
       LATERAL (SELECT t.o.o_orderkey o_orderkey, t.o.o_totalprice o_totalprice 
       FROM
       UNNEST(customer.c_orders) t(o)) orders 
       GROUP BY customer.c_custkey, customer.c_name, orders.o_orderkey 
       ORDER BY total_spending
       LIMIT 50;  

**Example 2**  

The queries in this example return the first 2000 orders of the top 50 customers for orders greater than one thousand dollars. 

*FLATTEN*   

       SELECT l.c_custkey, l.c_name, r.orderkey, r.totalprice 
       FROM customer l 
       LEFT OUTER JOIN (SELECT g.custkey custkey, g.name, g.orderkey, g.totalprice totalprice 
       FROM (SELECT row_number() OVER(PARTITION BY c_custkey) 
       AS rn, f.c_custkey custkey, f.c_name name, f.o.o_orderkey orderkey, f.o.o_totalprice totalprice 
       FROM (SELECT c_custkey, c_name, 
       FLATTEN(c_orders) 
       AS o 
       FROM customer) f 
       WHERE f.o.o_totalprice > 1000) g 
       WHERE rn < 2001) r 
       ON (l.c_custkey = r.custkey) 
       ORDER BY l.c_custkey, r.totalprice 
       LIMIT 50;

*LATERAL and UNNEST*   

       SELECT customer.c_custkey, customer.c_name, orders.o_orderkey, orders.o_totalprice 
       FROM customer 
       LEFT JOIN LATERAL (SELECT t.o.o_orderkey o_orderkey, t.o.o_totalprice o_totalprice 
       FROM
       UNNEST(customer.c_orders) t(o) 
       WHERE t.o.o_totalprice > 1000 LIMIT 2000) orders ON TRUE
       ORDER BY c_custkey, orders.o_totalprice
       LIMIT 50;  




       



 





         
 
 
