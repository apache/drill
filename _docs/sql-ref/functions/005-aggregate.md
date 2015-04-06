---
title: "Aggregate and Aggregate Statistical"
parent: "SQL Functions"
---

## Aggregate Functions

The following tables list the aggregate and aggregate statistical functions that you can use in 
Drill queries:

**Function** | **Argument Type** | **Return Type**  
  --------   |   -------------   |   -----------
avg(expression)| smallint, int, bigint, real, double precision, numeric, or interval| numeric for any integer-type argument, double precision for a floating-point argument, otherwise the same as the argument data type
count(*)| _-_| bigint
count([DISTINCT] expression)| any| bigint
max(expression)| any array, numeric, string, or date/time type| same as argument type
min(expression)| any array, numeric, string, or date/time type| same as argument type
sum(expression)| smallint, int, bigint, real, double precision, numeric, or interval| bigint for smallint or int arguments, numeric for bigint arguments, double precision for floating-point arguments, otherwise the same as the argument data type
  
  
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
  