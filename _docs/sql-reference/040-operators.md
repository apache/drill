---
title: "Operators"
date: 2017-03-14 05:06:04 UTC
parent: "SQL Reference"
---
You can use various types of operators in your Drill queries to perform
operations on your data.

## Logical Operators

You can use the following logical operators in your Drill queries:

  * AND
  * BETWEEN (Includes end points. For example, if a query states WHERE age BETWEEN 10 AND 20, Drill returns both 10 and 20 in the result.)
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

You can use the LIKE pattern matching operator in your Drill queries.

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

See [SELECT Statements]({{ site.baseurl }}/docs/select).

## String Concatenate Operator

You can use the following string operator in your Drill queries to concatenate strings:

  * string || string

The concatenate operator is an alternative to the [concat function]({{ site.baseurl }}/docs/string-manipulation/#concat) and will concatenate input if Drill can implicitly convert the input to a string.

The concat function treats NULL as an empty string. The concatenate operator (||) returns NULL if any input is NULL.

## Operator Precedence 

The following table shows the precedence of operators in decreasing order:

| Operator/Element                     | Associativity | Description                                                 |
|--------------------------------------|---------------|-------------------------------------------------------------|
| .                                    | left          | dot notation used, for example, to drill down in a JSON map |
| [ ]                                  | left          | array-style notation to drill down into a JSON array        |
| -                                    | right         | unary minus                                                 |
| E                                    | left          | exponentiation                                              |
| * / %                                | left          | multiplication, division, modulo                            |
| + -                                  | left          | addition, subtraction                                       |
| IS                                   |               | IS TRUE, IS FALSE, IS UNKNOWN, IS NULL                      |
| IS NULL                              |               | test for null                                               |
| IS NOT NULL                          |               | test for not null                                           |
| (any other)                          | left          | all other native and user-defined operators                 |
| IN                                   |               | set membership                                              |
| BETWEEN                              |               | range containment, includes end points                                            |
| OVERLAPS                             |               | time interval overlap                                       |
| LIKE ILIKE SIMILAR TO NOT SIMILAR TO |               | string pattern matching                                     |
| < >                                  |               | less than, greater than                                     |
| =                                    | right         | equality, assignment                                        |
| NOT                                  | right         | logical negation                                            |
| AND                                  | left          | logical conjunction                                         |
| OR                                   | left          | logical disjunction                                         |

