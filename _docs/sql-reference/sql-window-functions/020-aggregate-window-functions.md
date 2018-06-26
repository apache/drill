---
title: "Aggregate Window Functions"
date: 2018-06-26 01:02:27 UTC
parent: "SQL Window Functions"
---

Window functions operate on a set of rows and return a single value for each row from the underlying query. The OVER() clause differentiates window functions from other analytical and reporting functions. See [SQL Window Functions Introduction]({{site.baseurl}}/docs/sql-window-functions-introduction/). You can use certain aggregate functions as window functions in Drill. 

The following table lists the aggregate window functions with supported data types and descriptions:  

| **Window   Function** | **Argument Type**                                                                     | **Return Type**                                                                                                                      | **Description**                                                                                                                                                                                                                                                         |
|-------------------|-----------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| AVG()             | SMALLINT,   INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, INTERVALYEAR or INTERVALDAY  | DECIMAL   for DECIMAL argument, DOUBLE for all other arguments                                                                   | The   AVG window function returns the average value for the input expression   values. The AVG function works with numeric values and ignores NULL values.                                                                                                          |
| COUNT()           | All   argument data types                                                         | BIGINT                                                                                                                           | The   COUNT() window function counts the number of input rows. COUNT(*) counts all   of the rows in the target table if they do or do not include nulls.   COUNT(expression) computes the number of rows with non-NULL values in a   specific column or expression. |
| MAX()             | BINARY,   DECIMAL, VARCHAR, DATE, TIME, or TIMESTAMP                              | Same   as argument type                                                                                                          | The   MAX() window function returns the maximum value of the expression across all   input values. The MAX function works with numeric values and ignores NULL   values.                                                                                            |
| MIN()             | BINARY,   DECIMAL, VARCHAR, DATE, TIME, or TIMESTAMP                              | Same   as argument type                                                                                                          | The   MIN () window function returns the minimum value of the expression across all   input values. The MIN function works with numeric values and ignores NULL   values.                                                                                           |
| SUM()             | SMALLINT,   INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, INTERVALDAY, or INTERVALYEAR | DECIMAL for DECIMAL argument,     BIGINT for any integer-type argument (including BIGINT), DOUBLE for   floating-point arguments | The   SUM () window function returns the sum of the expression across all input   values. The SUM function works with numeric values and ignores NULL values.                                                                                                       |  

                                                                                                  

## Syntax  
       window_function ( [ ALL ] expression ) 
       OVER ( [ PARTITION BY expr_list ] [ ORDER BY order_list frame_clause ] )



## Arguments  

*window_function*  
One of the following supported aggregate functions:  
AVG(), COUNT(), MAX(), MIN(), SUM() 
 
*expression*  
The target column or expression that the function operates on.  

ALL  
When you include ALL, the function retains all duplicate values from the expression. ALL is the default. DISTINCT is not supported.  

OVER  
Specifies the window clauses for the aggregation functions. The OVER clause distinguishes window aggregation functions from normal set aggregation functions.  

PARTITION BY *expr_list*  
Defines the window for the window function in terms of one or more expressions.  

ORDER BY *order_list*  
Sorts the rows within each partition. If PARTITION BY is not specified, ORDER BY uses the entire table.  

*frame_clause*  
If an ORDER BY clause is used for an aggregate function, an explicit frame clause is required. The frame clause refines the set of rows in a function's window, including or excluding sets of rows within the ordered result. The frame clause consists of the ROWS or RANGE keyword and associated specifiers.
  


## Examples  
The following examples show queries that use each of the aggregate window functions in Drill. See [SQL Window Functions Examples]({{site.baseurl}}/docs/sql-window-functions-examples/) for information about the data and setup for these examples.
 

### AVG()  
The following query uses the AVG() window function with the PARTITION BY clause to calculate the average sales for each car dealer in Q1.  

       select dealer_id, sales, avg(sales) over (partition by dealer_id) as avgsales from q1_sales;
       +------------+--------+-----------+
       | dealer_id  | sales  | avgsales  |
       +------------+--------+-----------+
       | 1          | 19745  | 14357     |
       | 1          | 19745  | 14357     |
       | 1          | 8227   | 14357     |
       | 1          | 9710   | 14357     |
       | 2          | 16233  | 13925     |
       | 2          | 16233  | 13925     |
       | 2          | 9308   | 13925     |
       | 3          | 15427  | 12368     |
       | 3          | 12369  | 12368     |
       | 3          | 9308   | 12368     |
       +------------+--------+-----------+
       10 rows selected (0.455 seconds)

### COUNT()  
The following query uses the COUNT (*) window function to count the number of sales in Q1, ordered by dealer_id. The word count is enclosed in back ticks (``) because it is a reserved keyword in Drill.  

       select dealer_id, sales, count(*) over(order by dealer_id) as `count` from q1_sales;
       +------------+--------+--------+
       | dealer_id  | sales  | count  |
       +------------+--------+--------+
       | 1          | 19745  | 4      |
       | 1          | 19745  | 4      |
       | 1          | 8227   | 4      |
       | 1          | 9710   | 4      |
       | 2          | 16233  | 7      |
       | 2          | 16233  | 7      |
       | 2          | 9308   | 7      |
       | 3          | 15427  | 10     |
       | 3          | 12369  | 10     |
       | 3          | 9308   | 10     |
       +------------+--------+--------+
       10 rows selected (0.215 seconds) 

The following query uses the COUNT() window function to count the total number of sales for each dealer in Q1. 

       select dealer_id, sales, count(sales) over(partition by dealer_id) as `count` from q1_sales;
       +------------+--------+--------+
       | dealer_id  | sales  | count  |
       +------------+--------+--------+
       | 1          | 19745  | 4      |
       | 1          | 19745  | 4      |
       | 1          | 8227   | 4      |
       | 1          | 9710   | 4      |
       | 2          | 16233  | 3      |
       | 2          | 16233  | 3      |
       | 2          | 9308   | 3      |
       | 3          | 15427  | 3      |
       | 3          | 12369  | 3      |
       | 3          | 9308   | 3      |
       +------------+--------+--------+
       10 rows selected (0.249 seconds)


### MAX()  
The following query uses the MAX() window function with the PARTITION BY clause to identify the employee with the maximum number of car sales in Q1 at each dealership. The word max is a reserved keyword in Drill and must be enclosed in back ticks (``).  

       select emp_name, dealer_id, sales, max(sales) over(partition by dealer_id) as `max` from q1_sales;
       +-----------------+------------+--------+--------+
       |    emp_name     | dealer_id  | sales  |  max   |
       +-----------------+------------+--------+--------+
       | Ferris Brown    | 1          | 19745  | 19745  |
       | Noel Meyer      | 1          | 19745  | 19745  |
       | Raphael Hull    | 1          | 8227   | 19745  |
       | Jack Salazar    | 1          | 9710   | 19745  |
       | Beverly Lang    | 2          | 16233  | 16233  |
       | Kameko French   | 2          | 16233  | 16233  |
       | Haviva Montoya  | 2          | 9308   | 16233  |
       | Ursa George     | 3          | 15427  | 15427  |
       | Abel Kim        | 3          | 12369  | 15427  |
       | May Stout       | 3          | 9308   | 15427  |
       +-----------------+------------+--------+--------+
       10 rows selected (0.402 seconds)


### MIN()  

The following query uses the MIN() window function with the PARTITION BY clause to identify the employee with the minimum number of car sales in Q1 at each dealership. The word min is a reserved keyword in Drill and must be enclosed in back ticks (``).  

       select emp_name, dealer_id, sales, min(sales) over(partition by dealer_id) as `min` from q1_sales;
       +-----------------+------------+--------+-------+
       |    emp_name     | dealer_id  | sales  |  min  |
       +-----------------+------------+--------+-------+
       | Ferris Brown    | 1          | 19745  | 8227  |
       | Noel Meyer      | 1          | 19745  | 8227  |
       | Raphael Hull    | 1          | 8227   | 8227  |
       | Jack Salazar    | 1          | 9710   | 8227  |
       | Beverly Lang    | 2          | 16233  | 9308  |
       | Kameko French   | 2          | 16233  | 9308  |
       | Haviva Montoya  | 2          | 9308   | 9308  |
       | Ursa George     | 3          | 15427  | 9308  |
       | Abel Kim        | 3          | 12369  | 9308  |
       | May Stout       | 3          | 9308   | 9308  |
       +-----------------+------------+--------+-------+
       10 rows selected (0.194 seconds)

### SUM()  
The following query uses the SUM() window function to total the amount of sales for each dealer in Q1. The word sum is a reserved keyword in Drill and must be enclosed in back ticks (``).  

       select dealer_id, emp_name, sales, sum(sales) over(partition by dealer_id) as `sum` from q1_sales;
       +------------+-----------------+--------+--------+
       | dealer_id  |    emp_name     | sales  |  sum   |
       +------------+-----------------+--------+--------+
       | 1          | Ferris Brown    | 19745  | 57427  |
       | 1          | Noel Meyer      | 19745  | 57427  |
       | 1          | Raphael Hull    | 8227   | 57427  |
       | 1          | Jack Salazar    | 9710   | 57427  |
       | 2          | Beverly Lang    | 16233  | 41774  |
       | 2          | Kameko French   | 16233  | 41774  |
       | 2          | Haviva Montoya  | 9308   | 41774  |
       | 3          | Ursa George     | 15427  | 37104  |
       | 3          | Abel Kim        | 12369  | 37104  |
       | 3          | May Stout       | 9308   | 37104  |
       +------------+-----------------+--------+--------+
       10 rows selected (0.198 seconds)
       


       
              
       
       
       
       
