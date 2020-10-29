---
title: "SQL dialect compatibility functions"
date: 2020-08-10
parent: "SQL Functions"
---

Drill supports the following functions which are not part of standard SQL but are nevertheless widely used in popular SQL dialects.


## Table of SQL dialect compatibility functions

| Function                                | Argument type     | Return Type                      | Description                                                        |
|-----------------------------------------|-------------------|----------------------------------|--------------------------------------------------------------------|
| CHR( index )                            | INT               | CHAR                             | Returns the ASCII character at a given index                       |
| IF( condition, then_value, else_value ) | BOOLEAN, any, any | Least restrictive of input types | Returns then_value when condition is true otherwise else_value     |
| LEAST and GREATEST                      | any               | Least restrictive of input types | Returns the least (resp. greatest) value from amongst the inputs   |
| LEFT and RIGHT                          | VARCHAR, INT      | VARCHAR                          | Returns the leading (resp. trailing) substring of the given length |
| NVL( expr1, expr2 )                     | any               | Least restrictive of input types | Returns the first non-null value from amongst the inputs           |
| SPACE( length )                         | INT               | VARCHAR                          | Returns a string of spaces of the given length                     |

| TRANSLATE( string, search_chars, replacement_chars ) | VARCHAR           | VARCHAR                          | Replaces a set of characters in a string with the corresponding members of another set |


## CHR

Returns the ASCII character at _index_.

### CHR Syntax

    CHR( index )

### CHR Examples

    SELECT CHR(65);

    |--------|
    | EXPR$0 |
    |--------|
    | A      |
    |--------|

## IF

Returns `then_value` if `condition` is true otherwise `else_value` thereby offering a shorthand for a CASE statement.

### IF Syntax

    `IF`( condition, then_value, else_value )

### IF Usage Notes

1. The word "IF" is amongst those reserved by Drill meaning that it is necessary to enclose invocations of this function in backticks: ```IF`( ... )``.

### IF Examples

    SELECT `IF`( current_date < '2012-12-21', 'World still going', 'World has ended' );

    | EXPR$0          |
    |-----------------|
    | World has ended |


## LEAST and GREATEST

Returns the least (resp. greatest) value from amongst the inputs.

### LEAST and GREATEST Syntax

    LEAST( expr1[, expr2, ... expr_n] )
    GREATEST( expr1[, expr2, ... expr_n] )

### LEAST and GREATEST Usage Notes

1. Calling these functions with input of mixed types may produce undefined results.
2. Return NULL if any of the inputs are NULL.

### LEAST and GREATEST Examples

    SELECT GREATEST(1,2,3,4,5,4,3,2,1);

    |--------|
    | EXPR$0 |
    |--------|
    | 5      |
    |--------|

In the following example, recall that uppercase letters precede lowercase letters lexicographically.

    SELECT LEAST('a', 'b', 'c', 'D','E','F');

    |--------|
    | EXPR$0 |
    |--------|
    | D      |
    |--------|



## LEFT and RIGHT

Returns the substring of the input string which starts (resp. ends) at the beginning (resp. end) of the input has the given length.

### LEFT and RIGHT Syntax

    LEFT( expr, length )
    RIGHT( expr, length )

### LEFT and RIGHT Usage Notes

1. If _expr_ is null then null is returned.
2. If _length_ = 0 then the empty string is returned.
3. If _length_ is greater than the length of _expr_ then _expr_ is returned.  

### LEFT and RIGHT Examples

    SELECT LEFT('The quick brown fox...', 7);

    |---------|
    | EXPR$0  |
    |---------|
    | The qui |
    |---------|


    SELECT RIGHT('The quick brown fox...', 5);

    |--------|
    | EXPR$0 |
    |--------|
    | ox...  |
    |--------|



## NVL

Returns the first non-null value from amongst the inputs.

### NVL Syntax

    NVL( expr1, expr2 )

### NVL Usage Notes

1. Returns NULL if both of the inputs are NULL.

### NVL Examples

    SELECT NVL(CAST(NULL AS INT), 123);

    |--------|
    | EXPR$0 |
    |--------|
    | 123    |
    |--------|


## SPACE

Returns a string of spaces of the given length.

### SPACE Syntax

    SPACE( length )

### SPACE Usage Notes

1. Returns the empty string when _length_ <= 0.

### SPACE Examples

    SELECT 'Foo' || SPACE(10) || 'bar';

    |------------------|
    | EXPR$0           |
    |------------------|
    | Foo          bar |
    |------------------|

## TRANSLATE

Returns the input string with all occurrences of a specified set of characters replaced by the corresponding members of a specified set of replacement characters.

### TRANSLATE Syntax

    TRANSLATE( string, search_chars, replacement_chars )

### TRANSLATE Usage Notes

The characters in _search_chars_ and _replacement_chars_ are not delimited and are mapped to one another by their position.  When _replacement_chars_ contains more characters than _search_chars_ then the extra characters are ignored.  When _replacement_chars_ contains fewer characters then the extra characters in _search_chars_ are replaced with the empty string.

### TRANSLATE Examples

    SELECT TRANSLATE('[The|quick|brown|fox|jumps|...]', '[]|', '"" ') 

    |--------------------------------|
    | EXPR$0                         |
    |--------------------------------|
    | "The quick brown fox jumps..." |
    |--------------------------------|


