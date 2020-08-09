---
title: "SQL Window Functions Introduction"
date: 2020-08-08
parent: "SQL Window Functions"
---

Window functions operate on a set of rows and return a single value for each row from the underlying query. The term window describes the set of rows on which the function operates. A window function uses values from the rows in a window to calculate the returned values.  

When you use a window function in a query, define the window using the OVER() clause. The OVER() clause (window definition) differentiates window functions from other analytical and reporting functions. A query can include multiple window functions with the same or different window definitions. 

The OVER() clause has the following capabilities:  

* Defines window partitions to form groups of rows (PARTITION BY clause).
* Orders rows within a partition (ORDER BY clause).

For example, the following query uses the AVG() window function to calculate the average sales for employees in Q1:  

       select emp_name, dealer_id, sales, avg(sales) over() as avgsales from q1_sales;
       |-----------------|------------|--------|-----------|
       |    emp_name     | dealer_id  | sales  | avgsales  |
       |-----------------|------------|--------|-----------|
       | Beverly Lang    | 2          | 16233  | 13631     |
       | Kameko French   | 2          | 16233  | 13631     |
       | Ursa George     | 3          | 15427  | 13631     |
       | Ferris Brown    | 1          | 19745  | 13631     |
       | Noel Meyer      | 1          | 19745  | 13631     |
       | Abel Kim        | 3          | 12369  | 13631     |
       | Raphael Hull    | 1          | 8227   | 13631     |
       | Jack Salazar    | 1          | 9710   | 13631     |
       | May Stout       | 3          | 9308   | 13631     |
       | Haviva Montoya  | 2          | 9308   | 13631     |
       |-----------------|------------|--------|-----------|
       10 rows selected (0.213 seconds)


The AVG() window function operates on the rows defined in the window and returns a value for each row. 
To compare, you can run a query using the AVG() function as a standard set function:  

       select avg(sales) as avgsales from q1_sales;
       |-----------|
       | avgsales  |
       |-----------|
       | 13630.5   |
       |-----------|
       1 row selected (0.131 seconds)
  

The query returns one row with the average of all the values in the specified column instead of returning values for each row.  

You can also include the optional PARTITION BY and ORDER BY clauses in a query. The PARTITION BY clause subdivides the window into partitions. The ORDER BY clause defines the logical order of the rows within each partition of the result set. Window functions are applied to the rows within each partition and sorted according to the order specification.  

The following query uses the AVG() window function with the PARTITION BY clause to determine the average car sales for each dealer in Q1:  

       select emp_name, dealer_id, sales, avg(sales) over (partition by dealer_id) as avgsales from q1_sales;
       |-----------------|------------|--------|-----------|
       |    emp_name     | dealer_id  | sales  | avgsales  |
       |-----------------|------------|--------|-----------|
       | Ferris Brown    | 1          | 19745  | 14357     |
       | Noel Meyer      | 1          | 19745  | 14357     |
       | Raphael Hull    | 1          | 8227   | 14357     |
       | Jack Salazar    | 1          | 9710   | 14357     |
       | Beverly Lang    | 2          | 16233  | 13925     |
       | Kameko French   | 2          | 16233  | 13925     |
       | Haviva Montoya  | 2          | 9308   | 13925     |
       | Ursa George     | 3          | 15427  | 12368     |
       | Abel Kim        | 3          | 12369  | 12368     |
       | May Stout       | 3          | 9308   | 12368     |
       |-----------------|------------|--------|-----------|
       10 rows selected (0.215 seconds)  

The following query uses the AVG() and ROW_NUM() window functions to determine the average car sales for each dealer in Q1 and assign a row number to each row in a partition:  

       select dealer_id, sales, emp_name,row_number() over (partition by dealer_id order by sales) as `row`,avg(sales) over (partition by dealer_id) as avgsales from q1_sales;
       |------------|--------|-----------------|------|---------------|
       | dealer_id  | sales  |    emp_name     | row  |      avgsales |
       |------------|--------|-----------------|------|---------------|
       | 1          | 8227   | Raphael Hull    | 1    | 14356         |
       | 1          | 9710   | Jack Salazar    | 2    | 14356         |
       | 1          | 19745  | Ferris Brown    | 3    | 14356         |
       | 1          | 19745  | Noel Meyer      | 4    | 14356         |
       | 2          | 9308   | Haviva Montoya  | 1    | 13924         |
       | 2          | 16233  | Beverly Lang    | 2    | 13924         |
       | 2          | 16233  | Kameko French   | 3    | 13924         |
       | 3          | 9308   | May Stout       | 1    | 12368         |
       | 3          | 12369  | Abel Kim        | 2    | 12368         |
       | 3          | 15427  | Ursa George     | 3    | 12368         |
       |------------|--------|-----------------|------|---------------|
       10 rows selected (0.37 seconds)  


## Types of Window Functions  

Currently, Drill supports the following value, aggregate, and ranking window functions:  

[Value]({{site.baseurl}}/docs/value-window-functions/)

* [FIRST_VALUE()]({{site.baseurl}}/docs/value-window-functions/#first_value-|-last_value)
* [LAG()]({{site.baseurl}}/docs/value-window-functions/#lag-|-lead)
* [LAST_VALUE()]({{site.baseurl}}/docs/value-window-functions/#first_value-|-last_value)
* [LEAD()]({{site.baseurl}}/docs/value-window-functions/#lag-|-lead) 

[Aggregate]({{site.baseurl}}/docs/aggregate-window-functions/)   

* [AVG()]({{site.baseurl}}/docs/aggregate-window-functions/)
* [COUNT()]({{site.baseurl}}/docs/aggregate-window-functions/)
* [MAX()]({{site.baseurl}}/docs/aggregate-window-functions/)
* [MIN()]({{site.baseurl}}/docs/aggregate-window-functions/)
* [SUM()]({{site.baseurl}}/docs/aggregate-window-functions/)

[Ranking]({{site.baseurl}}/docs/ranking-window-functions/)  

* [CUME_DIST()]({{site.baseurl}}/docs/ranking-window-functions/)
* [DENSE_RANK()]({{site.baseurl}}/docs/ranking-window-functions/)
* [NTILE()]({{site.baseurl}}/docs/ranking-window-functions/)
* [PERCENT_RANK()]({{site.baseurl}}/docs/ranking-window-functions/)
* [RANK()]({{site.baseurl}}/docs/ranking-window-functions/)
* [ROW_NUMBER()]({{site.baseurl}}/docs/ranking-window-functions/)

All of the ranking functions depend on the sort ordering specified by the ORDER BY clause of the associated window definition. Rows that are not distinct in the ordering are called peers. The ranking functions are defined so that they give the same answer for any two peer rows.  

## Syntax  

       window_function (expression) OVER (
       [ PARTITION BY expr_list ]
       [ ORDER BY order_list ][ frame_clause ] )  

where function is one of the functions described, such as AVG(), and *expr_list* is:  

       expression | column_name [, expr_list ]

and *order_list* is:  

       expression | column_name [ASC | DESC] [ NULLS { FIRST | LAST } ] [, order_list ]

and the optional *frame_clause* is one of the following frames:

       { RANGE | ROWS } frame_start
       { RANGE | ROWS } BETWEEN frame_start AND frame_end  

where *frame_start* is one of the following choices: 
 
       UNBOUNDED PRECEDING  
       CURRENT ROW  

and *frame_end* is one of the following choices:  

       CURRENT ROW  
       UNBOUNDED FOLLOWING  

{% include startnote.html %}The `frame_end` choice cannot appear earlier than the `frame_start` choice and defaults to CURRENT ROW if not explicitly included.{% include endnote.html %}


## Arguments  

*window_function*  
Any of the following functions used with the OVER clause to provide a window specification:  

* AVG()
* COUNT()
* CUME_DIST()
* DENSE_RANK()
* FIRST_VALUE()
* LAG()
* LAST_VALUE()
* LEAD()
* MAX()
* MIN()
* NTILE()
* PERCENT_RANK()
* RANK()
* ROW_NUMBER()
* SUM()

OVER()  
OVER() is a mandatory clause that defines a window within a query result set. OVER() is a subset of SELECT and a part of the aggregate definition. A window function computes a value for each row in the window.  

PARTITION BY *expr_list*  
PARTITION BY is an optional clause that subdivides the data into partitions. Including the partition clause divides the query result set into partitions, and the window function is applied to each partition separately. Computation restarts for each partition. If you do not include a partition clause, the function calculates on the entire table or file.  

ORDER BY *order_list*  
The ORDER BY clause defines the logical order of the rows within each partition of the result set. If no PARTITION BY is specified, ORDER BY uses the entire table. ORDER BY is optional for the aggregate window functions and required for the ranking functions. This ORDER BY clause does not relate to the ORDER BY clause used outside of the OVER clause.  

The window function is applied to the rows within each partition sorted according to the order specification.  

Column identifiers or expressions that evaluate to column identifiers are required in the order list. You can also use constants as substitutes for column names.  

NULLS are treated as their own group, sorted and ranked last in ASC and sorted and ranked first in DESC. ASC is the default sort order.  

*column_name*  
The name of a column to be partitioned by or ordered by.  

ASC | DESC
Specifies sort order, either ascending or descending.  

*frame_clause*  
For window functions that operate on the frame instead of the whole partition, the frame\_clause specifies the group of rows that create the window frame. The frame\_clause supports the following frames:  

* RANGE UNBOUNDED PRECEDING
* RANGE BETWEEN CURRENT ROW AND CURRENT ROW
* [RANGE | ROWS] BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
* [RANGE | ROWS] BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  

The default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, which is the same as RANGE UNBOUNDED PRECEDING. This frame sets the frame as all rows from the start of the partition through the current row's last peer in the ordering, as specified by the ORDER BY clause.  The frame also includes ties when ordering is not unique.  

The following delimiters define the frame:  

* UNBOUNDED PRECEDING  
The frame starts with the first row of the partition.  
* UNBOUNDED FOLLOWING  
The frame ends with the last row of the partition, for both ROW and RANGE modes.  
* CURRENT ROW  
In ROWS mode, CURRENT ROW means that the frame starts or ends with the current row. In RANGE mode, CURRENT ROW means that the frame starts or ends with the current row’s first or last peer in the ORDER BY ordering.

## Usage Notes  

* You can only use window functions in the SELECT list and ORDER BY clauses of a query.  
* Window functions precede ORDER BY.  
* Drill processes window functions after the WHERE, GROUP BY, and HAVING clauses.  
* Including the OVER() clause after an aggregate set function turns the function into an aggregate window function. 
* You can use window functions to aggregate over any number of rows in the window frame.
* If you want to run a window function on the result set returned by the FLATTEN clause, use FLATTEN in a subquery. For example: ``select x, y, a, sum(x) over() from  ( select x , y, flatten(z) as a from `complex.json`); ``
