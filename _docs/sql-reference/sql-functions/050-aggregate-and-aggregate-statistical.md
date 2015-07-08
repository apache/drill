---
title: "Aggregate and Aggregate Statistical"
parent: "SQL Functions"
---

## Aggregate Functions

The following tables list the aggregate and aggregate statistical functions that you can use in 
Drill queries:

**Function** | **Argument Type** | **Return Type**  
  --------   |   -------------   |   -----------
AVG(expression)| SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, INTERVALYEAR or INTERVALDAY| DECIMAL for any integer-type argument, DOUBLE for a floating-point argument, otherwise the same as the argument data type
COUNT(*)| _-_| BIGINT
COUNT([DISTINCT] expression)| any| BIGINT
MAX(expression)| BINARY, DECIMAL, VARCHAR, DATE, TIME, or TIMESTAMP| same as argument type
MIN(expression)| BINARY, DECIMAL, VARCHAR, DATE, TIME, or TIMESTAMP| same as argument type
SUM(expression)| SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, INTERVALDAY, or INTERVALYEAR| BIGINT for SMALLINT or INTEGER arguments, DECIMAL for BIGINT arguments, DOUBLE for floating-point arguments, otherwise the same as the argument data type

\* In this release, Drill disables the DECIMAL data type, including casting to DECIMAL and reading DECIMAL types from Parquet and Hive. You can [enable the DECIMAL type](docs/supported-data-types/#enabling-the-decimal-type), but this is not recommended.

AVG, COUNT, MIN, MAX, and SUM accept ALL and DISTINCT keywords. The default is ALL.

## AVG 

Averages a column of all records in a data source. Averages a column of one or more groups of records. Which records to include in the calculation can be based on a condition.

### Syntax

    SELECT AVG(aggregate_expression)
    FROM tables
    WHERE conditions;

    SELECT expression1, expression2, ... expression_n,
           AVG(aggregate_expression)
    FROM tables
    WHERE conditions
    GROUP BY expression1, expression2, ... expression_n;

Expressions listed within the AVG function and must be included in the GROUP BY clause.

### Examples

    SELECT AVG(salary) FROM cp.`employee.json`;
    +---------------------+
    |       EXPR$0        |
    +---------------------+
    | 4019.6017316017314  |
    +---------------------+
    1 row selected (0.221 seconds)

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

## COUNT, MIN, MAX, and SUM

### Examples

    SELECT a2 FROM t2;
    +------------+
    |     a2     |
    +------------+
    | 0          |
    | 1          |
    | 2          |
    | 2          |
    | 2          |
    | 3          |
    | 4          |
    | 5          |
    | 6          |
    | 7          |
    | 7          |
    | 8          |
    | 9          |
    +------------+
    13 rows selected (0.056 seconds)

    SELECT AVG(ALL a2) FROM t2;
    +--------------------+
    |        EXPR$0      |
    +--------------------+
    | 4.3076923076923075 |
    +--------------------+
    1 row selected (0.084 seconds)

    SELECT AVG(DISTINCT a2) FROM t2;
    +------------+
    |   EXPR$0   |
    +------------+
    | 4.5        |
    +------------+
    1 row selected (0.079 seconds)

    SELECT SUM(ALL a2) FROM t2;
    +------------+
    |   EXPR$0   |
    +------------+
    | 56         |
    +------------+
    1 row selected (0.086 seconds)

    SELECT SUM(DISTINCT a2) FROM t2;
    +------------+
    |   EXPR$0   |
    +------------+
    | 45         |
    +------------+
    1 row selected (0.078 seconds)

    +------------+
    |   EXPR$0   |
    +------------+
    | 13         |
    +------------+
    1 row selected (0.056 seconds)

    SELECT COUNT(ALL a2) FROM t2;
    +------------+
    |   EXPR$0   |
    +------------+
    | 13         |
    +------------+
    1 row selected (0.056 seconds)

    SELECT COUNT(DISTINCT a2) FROM t2;
    +------------+
    |   EXPR$0   |
    +------------+
    | 10         |
    +------------+
    1 row selected (0.074 seconds)
  
  
## Aggregate Statistical Functions

Drill provides following aggregate statistics functions:

* stddev(expression) 
* stddev_pop(expression)
* stddev_samp(expression)
* variance(expression)
* var_pop(expression)
* var_samp(expression)
  
These functions take a SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, or DECIMAL expression as the argument. If the expression is FLOAT, the function returns  DOUBLE; otherwise, the function returns DECIMAL.
