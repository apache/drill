---
title: "Value Window Functions"
date: 2018-11-02
parent: "SQL Window Functions"
---
Window functions operate on a set of rows and return a single value for each row from the underlying query. The OVER() clause differentiates window functions from other analytical and reporting functions. See [SQL Window Functions Introduction]({{ site.baseurl }}/docs/sql-window-functions-introduction/).

The following table lists the value window functions with supported data types and descriptions:  

| Window Function | Argument Type                  | Return Type                  | Description                                                                                                                        |
|-----------------|--------------------------------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| LAG()           | Any supported Drill data types | Same as the expression  type | The LAG() window function returns the value for the row before the current row in a partition. If no row exists, null is returned. |
| LEAD()          | Any supported Drill data types | Same as the expression type  | The LEAD() window function returns the value for the row after the current row in a partition. If no row exists, null is returned. |
| FIRST_VALUE     | Any supported Drill data types | Same as the expression type  | The FIRST_VALUE window function returns the value of the specified expression with respect to the first row in the window frame.   |
| LAST_VALUE      | Any supported Drill data types | Same as the expression type  | The LAST_VALUE window function returns the value of the specified expression with respect to the last row in the window frame.     |  

##Syntax  

###LAG | LEAD  

       LAG | LEAD
       ( expression )
       OVER ( [ PARTITION BY expr_list ] [ ORDER BY order_list ] )  

###FIRST\_VALUE | LAST_VALUE  

       FIRST_VALUE | LAST_VALUE
       ( expression ) OVER
       ( [ PARTITION BY expr_list ] [ ORDER BY order_list ][ frame_clause ] )  

##Arguments  

*expression*  
The target column or expression that the function operates on.  

OVER  
Specifies the window partitioning and ordering. The OVER clause cannot contain a window frame specification.  

PARTITION BY  *expr_list*  
Defines the window for the window function in terms of one or more expressions.  

ORDER BY *order_list*  
Sorts the rows within each partition. If PARTITION BY is not specified, ORDER BY uses the entire table.  

*frame_clause*  
The frame clause refines the set of rows in a function's window, including or excluding sets of rows within the ordered result. The frame clause consists of the ROWS or RANGE keyword and associated specifiers.  

##Examples
The following examples show queries that use each of the value window functions in Drill.  

###LAG()
The following example uses the LAG window function to show the quantity of records sold to the Tower Records customer with customer ID 8  and the dates that customer 8 purchased records. To compare each sale with the previous sale for customer 8, the query returns the previous quantity sold for each sale. Since there is no purchase before 1976-01-25, the first previous quantity sold value is null. Note that the term "date" in the query is enclosed in back ticks because it is a reserved keyword in Drill.  

       select cust_id, `date`, qty_sold, lag(qty_sold,1) over (order by cust_id, `date`) as prev_qtysold from sales where cust_id = 8 order by cust_id, `date`;  
       
       +----------+-------------+-----------+---------------+
       | cust_id  |    date     | qty_sold  | prev_qtysold  |
       +----------+-------------+-----------+---------------+
       | 8        | 1976-01-25  | 2         | null          |
       | 8        | 1981-02-04  | 5         | 2             |
       | 8        | 1982-08-09  | 2         | 5             |
       | 8        | 1983-02-12  | 1         | 2             |
       | 8        | 1984-02-10  | 9         | 1             |
       +----------+-------------+-----------+---------------+
       5 rows selected (0.331 seconds)
 
###LEAD()  
The following example uses the LEAD window function to provide the commission for concert tickets with show ID 172 and the next commission for subsequent ticket sales. Since there is no commission after 40.00, the last next_comm value is null. Note that the term "date" in the query is enclosed in back ticks because it is a reserved keyword in Drill.  

       select show_id, `date`, commission, lead(commission,1) over (order by `date`) as next_comm from commission where show_id = 172;
       +----------+-------------+-------------+------------+
       | show_id  |    date     | commission  | next_comm  |
       +----------+-------------+-------------+------------+
       | 172      | 1979-01-01  | 29.20       | 29.50      |
       | 172      | 1979-01-01  | 29.50       | 8.25       |
       | 172      | 1979-01-01  | 8.25        | 15.50      |
       | 172      | 1979-01-01  | 15.50       | 10.25      |
       | 172      | 1979-01-01  | 10.25       | 4.40       |
       | 172      | 1979-01-01  | 4.40        | 80.20      |
       | 172      | 1979-01-01  | 80.20       | 90.10      |
       | 172      | 1979-01-02  | 90.10       | 25.50      |
       | 172      | 1979-01-02  | 25.50       | 50.00      |
       | 172      | 1979-01-02  | 50.00       | 20.20      |
       | 172      | 1979-01-02  | 20.20       | 40.00      |
       | 172      | 1979-01-02  | 40.00       | null       |
       +----------+-------------+-------------+------------+
       12 rows selected (0.241 seconds)
      
###FIRST_VALUE() 
The following example uses the FIRST_VALUE window function to identify the employee with the lowest sales for each dealer in Q1:

       select emp_name, dealer_id, sales, first_value(sales) over (partition by dealer_id order by sales) as dealer_low from q1_sales;
       +-----------------+------------+--------+-------------+
       |    emp_name     | dealer_id  | sales  | dealer_low  |
       +-----------------+------------+--------+-------------+
       | Raphael Hull    | 1          | 8227   | 8227        |
       | Jack Salazar    | 1          | 9710   | 8227        |
       | Ferris Brown    | 1          | 19745  | 8227        |
       | Noel Meyer      | 1          | 19745  | 8227        |
       | Haviva Montoya  | 2          | 9308   | 9308        |
       | Beverly Lang    | 2          | 16233  | 9308        |
       | Kameko French   | 2          | 16233  | 9308        |
       | May Stout       | 3          | 9308   | 9308        |
       | Abel Kim        | 3          | 12369  | 9308        |
       | Ursa George     | 3          | 15427  | 9308        |
       +-----------------+------------+--------+-------------+
       10 rows selected (0.299 seconds)


###LAST_VALUE()
The following example uses the LAST_VALUE window function to identify the last car sale each employee made at each dealership in 2013:

       select emp_name, dealer_id, sales, `year`, last_value(sales) over (partition by  emp_name order by `year`) as last_sale from emp_sales where `year` = 2013;
       +-----------------+------------+--------+-------+------------+
       |    emp_name     | dealer_id  | sales  | year  | last_sale  |
       +-----------------+------------+--------+-------+------------+
       | Beverly Lang    | 2          | 5324   | 2013  | 5324       |
       | Ferris Brown    | 1          | 22003  | 2013  | 22003      |
       | Haviva Montoya  | 2          | 6345   | 2013  | 13100      |
       | Haviva Montoya  | 2          | 13100  | 2013  | 13100      |
       | Kameko French   | 2          | 7540   | 2013  | 7540       |
       | May Stout       | 2          | 4924   | 2013  | 15000      |
       | May Stout       | 2          | 8000   | 2013  | 15000      |
       | May Stout       | 2          | 15000  | 2013  | 15000      |
       | Noel Meyer      | 1          | 13314  | 2013  | 13314      |
       | Raphael Hull    | 1          | -4000  | 2013  | 14000      |
       | Raphael Hull    | 1          | 14000  | 2013  | 14000      |
       | Ursa George     | 1          | 10865  | 2013  | 10865      |
       +-----------------+------------+--------+-------+------------+
       12 rows selected (0.284 seconds)


