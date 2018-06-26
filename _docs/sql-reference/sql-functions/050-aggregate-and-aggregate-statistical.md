---
title: "Aggregate and Aggregate Statistical"
date: 2018-06-26 00:42:19 UTC
parent: "SQL Functions"
---

## Aggregate Functions

The following tables list the aggregate and aggregate statistical functions that you can use in 
Drill queries:  

| **Function**                     | **Argument Type**                                                 | **Return Type**                                                                                                                        |
|------------------------------|---------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| AVG(expression)              | SMALLINT,   INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, INTERVAL | DECIMAL for DECIMAL argument,   DOUBLE for all other arguments                                                                     |
| COUNT(*)                     | -                                                             | BIGINT                                                                                                                             |
| COUNT([DISTINCT] expression) | any                                                           | BIGINT                                                                                                                             |
| MAX(expression)              | BINARY,   DECIMAL, VARCHAR, DATE, TIME, or TIMESTAMP          | same   as argument type                                                                                                            |
| MIN(expression)              | BINARY,   DECIMAL, VARCHAR, DATE, TIME, or TIMESTAMP          | same   as argument type                                                                                                            |
| SUM(expression)              | SMALLINT,   INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, INTERVAL | DECIMAL for DECIMAL   argument,     BIGINT for any integer-type argument (including BIGINT), DOUBLE for   floating-point arguments |  

Starting in Drill 1.14, the DECIMAL data type is enabled by default. 

AVG, COUNT, MIN, MAX, and SUM accept ALL and DISTINCT keywords. The default is ALL.

These examples of aggregate functions use the `cp` storage plugin to access a the [`employee.json`]({{site.baseurl}}/docs/querying-json-files/) file installed with Drill. By default, JSON reads numbers as double-precision floating point numbers. These examples assume that you are using the default option [all_text_mode]({{site.baseurl}}/docs/json-data-model/#handling-type-differences) set to false.

## AVG 

Averages a column of all records in a data source. Averages a column of one or more groups of records. Which records to include in the calculation can be based on a condition.

### AVG Syntax

    SELECT AVG([ALL | DISTINCT] aggregate_expression)
    FROM tables
    WHERE conditions;

    SELECT expression1, expression2, ... expression_n,
           AVG([ALL | DISTINCT] aggregate_expression)
    FROM tables
    WHERE conditions
    GROUP BY expression1, expression2, ... expression_n;

Expressions listed within the AVG function and must be included in the GROUP BY clause. 

### AVG Examples

```
ALTER SESSION SET `store.json.all_text_mode` = false;
+-------+------------------------------------+
|  ok   |              summary               |
+-------+------------------------------------+
| true  | store.json.all_text_mode updated.  |
+-------+------------------------------------+
1 row selected (0.073 seconds)
```

Take a look at the salaries of employees having IDs 1139, 1140, and 1141. These are the salaries that subsequent examples will average and sum.

```
SELECT * FROM cp.`employee.json` WHERE employee_id IN (1139, 1140, 1141);
+--------------+------------------+-------------+------------+--------------+--------------------------+-----------+----------------+-------------+------------------------+-------------+----------------+------------------+-----------------+---------+-----------------------+
| employee_id  |    full_name     | first_name  | last_name  | position_id  |      position_title      | store_id  | department_id  | birth_date  |       hire_date        |   salary    | supervisor_id  | education_level  | marital_status  | gender  |    management_role    |
+--------------+------------------+-------------+------------+--------------+--------------------------+-----------+----------------+-------------+------------------------+-------------+----------------+------------------+-----------------+---------+-----------------------+
| 1139         | Jeanette Belsey  | Jeanette    | Belsey     | 12           | Store Assistant Manager  | 18        | 11             | 1972-05-12  | 1998-01-01 00:00:00.0  | 10000.0000  | 17             | Graduate Degree  | S               | M       | Store Management      |
| 1140         | Mona Jaramillo   | Mona        | Jaramillo  | 13           | Store Shift Supervisor   | 18        | 11             | 1961-09-24  | 1998-01-01 00:00:00.0  | 8900.0000   | 1139           | Partial College  | S               | M       | Store Management      |
| 1141         | James Compagno   | James       | Compagno   | 15           | Store Permanent Checker  | 18        | 15             | 1914-02-02  | 1998-01-01 00:00:00.0  | 6400.0000   | 1139           | Graduate Degree  | S               | M       | Store Full Time Staf  |
+--------------+------------------+-------------+------------+--------------+--------------------------+-----------+----------------+-------------+------------------------+-------------+----------------+------------------+-----------------+---------+-----------------------+
3 rows selected (0.284 seconds)
```

```
SELECT AVG(salary) FROM cp.`employee.json` WHERE employee_id IN (1139, 1140, 1141);
+--------------------+
|       EXPR$0       |
+--------------------+
| 8433.333333333334  |
+--------------------+
1 row selected (0.208 seconds)

SELECT AVG(ALL salary) FROM cp.`employee.json` WHERE employee_id IN (1139, 1140, 1141);
+--------------------+
|       EXPR$0       |
+--------------------+
| 8433.333333333334  |
+--------------------+
1 row selected (0.17 seconds)

SELECT AVG(DISTINCT salary) FROM cp.`employee.json`;
+---------------------+
|       EXPR$0        |
+---------------------+
| 12773.333333333334  |
+---------------------+
1 row selected (0.384 seconds)
```

    SELECT education_level, AVG(salary) FROM cp.`employee.json` GROUP BY education_level;
    +----------------------+---------------------+
    |   education_level    |       EXPR$1        |
    +----------------------+---------------------+
    | Graduate Degree      | 4392.823529411765   |
    | Bachelors Degree     | 4492.404181184669   |
    | Partial College      | 4047.1180555555557  |
    | High School Degree   | 3516.1565836298932  |
    | Partial High School  | 3511.0852713178297  |
    +----------------------+---------------------+
    5 rows selected (0.495 seconds)

## COUNT
Returns the number of rows that match the given criteria.

### COUNT Syntax

`SELECT COUNT([DISTINCT | ALL] column) FROM . . .`  
`SELECT COUNT(*) FROM . . .`  

* column  
  Returns the number of values of the specified column.  
* DISTINCT column  
  Returns the number of distinct values in the column.  
* ALL column  
  Returns the number of values of the specified column.  
* * (asterisk)
  Returns the number of records in the table.


### COUNT Examples

    SELECT COUNT(DISTINCT salary) FROM cp.`employee.json`;
    +---------+
    | EXPR$0  |
    +---------+
    | 48      |
    +---------+
    1 row selected (0.159 seconds)

    SELECT COUNT(ALL salary) FROM cp.`employee.json`;
    +---------+
    | EXPR$0  |
    +---------+
    | 1155    |
    +---------+
    1 row selected (0.106 seconds)

    SELECT COUNT(salary) FROM cp.`employee.json`;
    +---------+
    | EXPR$0  |
    +---------+
    | 1155    |
    +---------+
    1 row selected (0.102 seconds)

    SELECT COUNT(*) FROM cp.`employee.json`;
    +---------+
    | EXPR$0  |
    +---------+
    | 1155    |
    +---------+
    1 row selected (0.174 seconds)

## MIN and MAX Functions
These functions return the smallest and largest values of the selected columns, respectively.

### MIN and MAX Syntax

MIN(column)  
MAX(column)

### MIN and MAX Examples

```
SELECT MIN(salary) FROM cp.`employee.json`;
+---------+
| EXPR$0  |
+---------+
| 20.0    |
+---------+
1 row selected (0.138 seconds)

SELECT MAX(salary) FROM cp.`employee.json`;
+----------+
|  EXPR$0  |
+----------+
| 80000.0  |
+----------+
1 row selected (0.139 seconds)
```

Use a correlated subquery to find the names and salaries of the lowest paid employees:

```
SELECT full_name, SALARY FROM cp.`employee.json` WHERE salary = (SELECT MIN(salary) FROM cp.`employee.json`);
+------------------------+---------+
|       full_name        | SALARY  |
+------------------------+---------+
| Leopoldo Renfro        | 20.0    |
| Donna Brockett         | 20.0    |
| Laurie Anderson        | 20.0    |
. . .
```

## SUM Function
Returns the total of a numeric column.

### SUM syntax

`SUM(column)`

### Examples

```
SELECT SUM(ALL salary) FROM cp.`employee.json`;
+------------+
|   EXPR$0   |
+------------+
| 4642640.0  |
+------------+
1 row selected (0.123 seconds)

SELECT SUM(DISTINCT salary) FROM cp.`employee.json`;
+-----------+
|  EXPR$0   |
+-----------+
| 613120.0  |
+-----------+
1 row selected (0.309 seconds)

SELECT SUM(salary) FROM cp.`employee.json` WHERE employee_id IN (1139, 1140, 1141);
+----------+
|  EXPR$0  |
+----------+
| 25300.0  |
+----------+
1 row selected (1.995 seconds)
```

## Aggregate Statistical Functions

Drill provides following aggregate statistics functions:

* stddev(expression)  
  An alias for stddev_samp
* stddev_pop(expression)
  Population standard deviate of input values
* stddev_samp(expression)
  Sample standard deviate of input values
* variance(expression)
  An alias for var_samp
* var_pop(expression)
  Population variance of input values (the population standard deviated squared)
* var_samp(expression)
  Sample variance of input values (sample standard deviation squared)
  
These functions take a SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, or DECIMAL expression as the argument. The functions return DECIMAL for DECIMAL arguments and DOUBLE for all other arguments.