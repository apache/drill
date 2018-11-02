---
title: "Functions for Handling Nulls"
date: 2018-11-02
parent: "SQL Functions"
---

Drill supports the following functions for handling nulls:

* COALESCE
* NULLIF

## COALESCE
Returns the first non-null expression in the list. 

### COALESCE Syntax

    COALESCE( expr1[, expr2, ... expr_n] )

*expr1* to *expr_n* are any valid scalar expressions.

## COALESCE Usage Notes
If all expressions evaluate to null, then the COALESCE function returns null. Expressions have to be of the same type.

## NULLIF
Returns the first expression if the two expressions are not equal, or 
returns a null value of the type of the first expression if the two expressions are equal.

### NULLIF Syntax

    NULLIF ( expr1, expr2 )

*expr1* to *expr2* are any valid scalar expressions.

This function returns the same type as the first expression.

### NULLIF Examples

    SELECT d9, d18 FROM alltypes LIMIT 1;
    +------------+------------+
    |     d9     |    d18     |
    +------------+------------+
    | 1032.65    | 1032.6516  |
    +------------+------------+
    1 row selected (0.081 seconds)

    SELECT NULLIF(d9, d18) FROM alltypes limit 1;
    +------------+
    |   EXPR$0   |
    +------------+
    | 1032.65    |
    +------------+
    1 row selected (0.079 seconds)

    SELECT NULLIF(d9, d9) FROM alltypes limit 1;
    +------------+
    |   EXPR$0   |
    +------------+
