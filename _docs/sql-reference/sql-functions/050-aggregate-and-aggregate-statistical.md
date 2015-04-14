---
title: "Aggregate and Aggregate Statistical"
parent: "SQL Functions"
---

## Aggregate Functions

The following tables list the aggregate and aggregate statistical functions that you can use in 
Drill queries:

**Function** | **Argument Type** | **Return Type**  
  --------   |   -------------   |   -----------
AVG(expression)| smallint, int, bigint, real, double precision, numeric, or interval| numeric for any integer-type argument, double precision for a floating-point argument, otherwise the same as the argument data type
COUNT(*)| _-_| bigint
COUNT([DISTINCT] expression)| any| bigint
MAX(expression)| any array, numeric, string, or date/time type| same as argument type
MIN(expression)| any array, numeric, string, or date/time type| same as argument type
SUM(expression)| smallint, int, bigint, real, double precision, numeric, or interval| bigint for smallint or int arguments, numeric for bigint arguments, double precision for floating-point arguments, otherwise the same as the argument data type

MIN, MAX, COUNT, AVG, and SUM accept ALL and DISTINCT keywords. The default is ALL.

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
    +------------+
    |   EXPR$0   |
    +------------+
    | 4.3076923076923075 |
    +------------+
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

The following table provides the aggregate statistics functions that you can use in your Drill queries:

**Function**| **Argument Type**| **Return Type**
  --------  |   -------------  |   -----------
stddev(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
stddev_pop(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
stddev_samp(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
variance(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
var_pop(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
var_samp(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
  