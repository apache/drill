---
title: "Ranking Window Functions"
date: 2018-11-02
parent: "SQL Window Functions"
---

Window functions operate on a set of rows and return a single value for each row from the underlying query. The OVER() clause differentiates window functions from other analytical and reporting functions. See [SQL Window Functions Introduction]({{ site.baseurl }}/docs/sql-window-functions-introduction/). You can use ranking functions in Drill to return a ranking value for each row in a partition.  

The following table lists the ranking window functions with supported data types and descriptions:  

| Window Function | Return Type      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|-----------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CUME_DIST()     | DOUBLE PRECISION | The CUME_DIST() window function calculates the relative rank of the current row within a window partition: (number of rows preceding or peer with current row) / (total rows in the window partition)                                                                                                                                                                                                                                                                                                                                                 |
| DENSE_RANK()    | BIGINT           | The DENSE_RANK () window function determines the rank of a value in a group of values based on the ORDER BY expression and the OVER clause. Each value is ranked within its partition. Rows with equal values receive the same rank. There are no gaps in the sequence of ranked values if two or more rows have the same rank.                                                                                                                                                                                                                       |
| NTILE()         | INTEGER          | The NTILE window function divides the rows for each window partition, as equally as possible, into a specified number of ranked groups. The NTILE window function requires the ORDER BY clause in the OVER clause.                                                                                                                                                                                                                                                                                                                                    |
| PERCENT_RANK()  | DOUBLE PRECISION | The PERCENT_RANK () window function calculates the percent rank of the current row using the following formula: (x - 1) / (number of rows in window partition - 1) where x is the rank of the current row.                                                                                                                                                                                                                                                                                                                                            |
| RANK()          | BIGINT           | The RANK window function determines the rank of a value in a group of values. The ORDER BY expression in the OVER clause determines the value. Each value is ranked within its partition. Rows with equal values for the ranking criteria receive the same rank. Drill adds the number of tied rows to the tied rank to calculate the next rank and thus the ranks might not be consecutive numbers. For example, if two rows are ranked 1, the next rank is 3. The DENSE_RANK window function differs in that no gaps exist if two or more rows tie. |
| ROW_NUMBER()    | BIGINT           | The ROW_NUMBER window function determines the ordinal number of the current row within its partition. The ORDER BY expression in the OVER clause determines the number. Each value is ordered within its partition. Rows with equal values for the ORDER BY expressions receive different row numbers nondeterministically.                                                                                                                                                                                                                           |  



## Syntax  

       window_function () OVER clause



## Arguments  

*window\_function*  
One of the following supported ranking functions:  
CUME_DIST(), DENSE_RANK(), PERCENT_RANK(), RANK(), ROW_NUMBER() 
 
( )  
The functions do not take arguments, however the empty parentheses are required.  

OVER clause  
The window clauses for the function. The OVER clause cannot contain an explicit frame specification, but must include an ORDER BY clause. See [Window Function Syntax]({{ site.baseurl }}/docs/sql-window-functions-introduction/#syntax) for OVER clause syntax.



## Examples  
The following examples show queries that use each of the ranking window functions in Drill. See [Window Functions Examples]({{ site.baseurl }}/docs/sql-window-functions-examples/) for information about the data and setup for these examples.
 

### CUME_DIST()  
The following query uses the CUME_DIST() window function to calculate the cumulative distribution of sales for each dealer in Q1.  

       select dealer_id, sales, cume_dist() over(order by sales) as cumedist from q1_sales;
       +------------+--------+-----------+
       | dealer_id  | sales  | cumedist  |
       +------------+--------+-----------+
       | 1          | 8227   | 0.1       |
       | 3          | 9308   | 0.3       |
       | 2          | 9308   | 0.3       |
       | 1          | 9710   | 0.4       |
       | 3          | 12369  | 0.5       |
       | 3          | 15427  | 0.6       |
       | 2          | 16233  | 0.8       |
       | 2          | 16233  | 0.8       |
       | 1          | 19745  | 1.0       |
       | 1          | 19745  | 1.0       |
       +------------+--------+-----------+
       10 rows selected (0.241 seconds)  

### DENSE_RANK  

The following query uses the DENSE_RANK() window function to rank the employee sales in Q1.  

       select dealer_id, emp_name, sales, dense_rank() over(order by sales) as denserank from q1_sales; 
       +------------+-----------------+--------+------------+
       | dealer_id  |    emp_name     | sales  | denserank  |
       +------------+-----------------+--------+------------+
       | 1          | Raphael Hull    | 8227   | 1          |
       | 3          | May Stout       | 9308   | 2          |
       | 2          | Haviva Montoya  | 9308   | 2          |
       | 1          | Jack Salazar    | 9710   | 3          |
       | 3          | Abel Kim        | 12369  | 4          |
       | 3          | Ursa George     | 15427  | 5          |
       | 2          | Beverly Lang    | 16233  | 6          |
       | 2          | Kameko French   | 16233  | 6          |
       | 1          | Ferris Brown    | 19745  | 7          |
       | 1          | Noel Meyer      | 19745  | 7          |
       +------------+-----------------+--------+------------+
       10 rows selected (0.198 seconds)  

###NTILE()  

The following example uses the NTILE window function to divide the Q1 sales into five groups and list the sales in ascending order.

       select emp_mgr, sales, ntile(5) over(order by sales) as ntilerank from q1_sales;
       +-----------------+--------+------------+
       |     emp_mgr     | sales  | ntilerank  |
       +-----------------+--------+------------+
       | Kari Phelps     | 8227   | 1          |
       | Rich Hernandez  | 9308   | 1          |
       | Kari Phelps     | 9710   | 2          |
       | Rich Hernandez  | 12369  | 2          |
       | Mike Palomino   | 13181  | 3          |
       | Rich Hernandez  | 15427  | 3          |
       | Kari Phelps     | 15547  | 4          |
       | Mike Palomino   | 16233  | 4          |
       | Dan Brodi       | 19745  | 5          |
       | Mike Palomino   | 23176  | 5          |
       +-----------------+--------+------------+
       10 rows selected (0.149 seconds)

The following example partitions sales by dealer_id and uses the NTILE window function to divide the rows in each partition into three groups; dealer 1 had one remainder which was added to the first group.

       select emp_mgr, dealer_id, sales, ntile(3) over(partition by dealer_id order by sales) as ntilerank from q1_sales;
       +-----------------+------------+--------+------------+
       |     emp_mgr     | dealer_id  | sales  | ntilerank  |
       +-----------------+------------+--------+------------+
       | Kari Phelps     | 1          | 8227   | 1          |
       | Kari Phelps     | 1          | 9710   | 1          |
       | Kari Phelps     | 1          | 15547  | 2          |
       | Dan Brodi       | 1          | 19745  | 3          |
       | Mike Palomino   | 2          | 13181  | 1          |
       | Mike Palomino   | 2          | 16233  | 2          |
       | Mike Palomino   | 2          | 23176  | 3          |
       | Rich Hernandez  | 3          | 9308   | 1          |
       | Rich Hernandez  | 3          | 12369  | 2          |
       | Rich Hernandez  | 3          | 15427  | 3          |
       +-----------------+------------+--------+------------+
       10 rows selected (0.312 seconds)


### PERCENT_RANK()  

The following query uses the PERCENT_RANK() window function to calculate the percent rank for employee sales in Q1.  

       select dealer_id, emp_name, sales, percent_rank() over(order by sales) as perrank from q1_sales; 
       +------------+-----------------+--------+---------------------+
       | dealer_id  |    emp_name     | sales  |       perrank       |
       +------------+-----------------+--------+---------------------+
       | 1          | Raphael Hull    | 8227   | 0.0                 |
       | 3          | May Stout       | 9308   | 0.1111111111111111  |
       | 2          | Haviva Montoya  | 9308   | 0.1111111111111111  |
       | 1          | Jack Salazar    | 9710   | 0.3333333333333333  |
       | 3          | Abel Kim        | 12369  | 0.4444444444444444  |
       | 3          | Ursa George     | 15427  | 0.5555555555555556  |
       | 2          | Beverly Lang    | 16233  | 0.6666666666666666  |
       | 2          | Kameko French   | 16233  | 0.6666666666666666  |
       | 1          | Ferris Brown    | 19745  | 0.8888888888888888  |
       | 1          | Noel Meyer      | 19745  | 0.8888888888888888  |
       +------------+-----------------+--------+---------------------+
       10 rows selected (0.169 seconds)

### RANK()  

The following query uses the RANK() window function to rank the employee sales for Q1. The word rank in Drill is a reserved keyword and must be enclosed in back ticks (``).
 
       select dealer_id, emp_name, sales, rank() over(order by sales) as `rank` from q1_sales;
       +------------+-----------------+--------+-------+
       | dealer_id  |    emp_name     | sales  | rank  |
       +------------+-----------------+--------+-------+
       | 1          | Raphael Hull    | 8227   | 1     |
       | 3          | May Stout       | 9308   | 2     |
       | 2          | Haviva Montoya  | 9308   | 2     |
       | 1          | Jack Salazar    | 9710   | 4     |
       | 3          | Abel Kim        | 12369  | 5     |
       | 3          | Ursa George     | 15427  | 6     |
       | 2          | Beverly Lang    | 16233  | 7     |
       | 2          | Kameko French   | 16233  | 7     |
       | 1          | Ferris Brown    | 19745  | 9     |
       | 1          | Noel Meyer      | 19745  | 9     |
       +------------+-----------------+--------+-------+
       10 rows selected (0.174 seconds)

### ROW_NUMBER()  

The following query uses the ROW_NUMBER() window function to number the sales for each dealer_id. The word rownum contains the reserved keyword row and must be enclosed in back ticks (``).  

        select dealer_id, emp_name, sales, row_number() over(partition by dealer_id order by sales) as `rownum` from q1_sales;
       +------------+-----------------+--------+---------+
       | dealer_id  |    emp_name     | sales  | rownum  |
       +------------+-----------------+--------+---------+
       | 1          | Raphael Hull    | 8227   | 1       |
       | 1          | Jack Salazar    | 9710   | 2       |
       | 1          | Ferris Brown    | 19745  | 3       |
       | 1          | Noel Meyer      | 19745  | 4       |
       | 2          | Haviva Montoya  | 9308   | 1       |
       | 2          | Beverly Lang    | 16233  | 2       |
       | 2          | Kameko French   | 16233  | 3       |
       | 3          | May Stout       | 9308   | 1       |
       | 3          | Abel Kim        | 12369  | 2       |
       | 3          | Ursa George     | 15427  | 3       |
       +------------+-----------------+--------+---------+
       10 rows selected (0.241 seconds)
              
       

 

      
