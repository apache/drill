---
title: "Operators"
parent: "SQL Reference"
---
You can use various types of operators in your Drill queries to perform
operations on your data.

## Logical Operators

You can use the following logical operators in your Drill queries:

  * AND
  * BETWEEN
  * IN
  * LIKE
  * NOT
  * OR 

## Comparison Operators

You can use the following comparison operators in your Drill queries:

  * <
  * \>
  * <=
  * \>=
  * =
  * <>
  * IS NULL
  * IS NOT NULL
  * IS FALSE 
  * IS NOT FALSE
  * IS TRUE 
  * IS NOT TRUE

## Pattern Matching Operators

You can use the following pattern matching operators in your Drill queries:

  * LIKE
  * NOT LIKE
  * SIMILAR TO
  * NOT SIMILAR TO

## Math Operators

You can use the following math operators in your Drill queries:

**Operator**| **Description**  
---|---  
+| Addition  
-| Subtraction  
*| Multiplication  
/| Division  
  
## Subquery Operators

You can use the following subquery operators in your Drill queries:

  * EXISTS
  * IN

See [SELECT Statements]({{ site.baseurl }}/docs/select-statements).

## String Concatenate Operator

You can use the following string operators in your Drill queries to concatenate strings:

  * string || string
  * string || non-string or non-string || string

The concatenate operator is an alternative to the [concat function]({{ site.baseurl }}/docs/string-manipulation#concat).

The concat function treats NULL as an empty string. The concatenate operator (||) returns NULL if any input is NULL.

