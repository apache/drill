---
title: "Math and Trig"
date: 2018-06-27 20:53:00 UTC
parent: "SQL Functions"
---
Drill supports the math functions shown in the following table of math functions plus trig functions listed at the end of this section. Most math functions and all trig functions take these input types:

* INTEGER
* BIGINT
* FLOAT
* DOUBLE
* SMALLINT*  
* DECIMAL**

\* Not supported. Drill treats SMALLINT as INT when reading from Parquet.  
\** Drill implicitly casts DECIMAL to DOUBLE for functions that take DOUBLE.  

Exceptions are the LSHIFT and RSHIFT functions, which take all types except FLOAT and DOUBLE types. DEGREES, EXP, RADIANS, and the multiple LOG functions take the input types in this list plus the DECIMAL type. 

## Table of Math Functions

| Function     | Return Type   | Description                                                               |
|--------------|---------------|---------------------------------------------------------------------------|
| ABS(x)       | Same as input | Returns the absolute value of the input argument x.                       |
| CBRT(x)      | FLOAT8        | Returns the cubic root of x.                                              |
| CEIL(x)      | Same as input | Returns the smallest integer not less than x.                             |
| CEILING(x)   | Same as input | Same as CEIL.                                                             |
| DEGREES(x)   | FLOAT8        | Converts x radians to degrees.                                            |
| E()          | FLOAT8        | Returns 2.718281828459045.                                                |
| EXP(x)       | FLOAT8        | Returns e (Euler's number) to the power of x.                             |
| FLOOR(x)     | Same as input | Returns the largest integer not greater than x.                           |
| LOG(x)       | FLOAT8        | Returns the natural log (base e) of x.                                    |
| LOG(x, y)    | FLOAT8        | Returns log base x to the y power.                                        |
| LOG10(x)     | FLOAT8        | Returns the common log of x.                                              |
| LSHIFT(x, y) | Same as input | Shifts the binary x by y times to the left.                               |
| MOD(x, y)    | FLOAT8        | Returns the remainder of x divided by y.                                  |
| NEGATIVE(x)  | Same as input | Returns x as a negative number.                                           |
| PI           | FLOAT8        | Returns pi.                                                               |
| POW(x, y)    | FLOAT8        | Returns the value of x to the y power.                                    |
| RADIANS(x)   | FLOAT8        | Converts x degrees to radians.                                            |
| RAND         | FLOAT8        | Returns a random number from 0-1.                                         |
| ROUND(x)     | Same as input | Rounds to the nearest integer.                                            |
| ROUND(x, y)  | DECIMAL       | Rounds x to y decimal places.                                             |
| RSHIFT(x, y) | Same as input | Shifts the binary x by y times to the right.                              |
| SIGN(x)      | INT           | Returns the sign of x.                                                    |
| SQRT(x)      | Same as input | Returns the square root of x.                                             |
| TRUNC(x, y)  | Same as input | Truncates x to y decimal places. Specifying y is optional. Default is 1.  |
| TRUNC(x, y)  | DECIMAL       | Truncates x to y decimal places.                                          |

## Math Function Examples

Examples in this section use the `input2.json` file. Download the `input2.json` file from the [Drill source code](https://github.com/apache/drill/tree/master/exec/java-exec/src/test/resources/jsoninput) page. You need to use a FROM clause in Drill queries. 

### ABS Example
Get the absolute value of the integer key in `input2.json`. The following snippet of input2.json shows the relevant integer content:

    { "integer" : 2010,
      "float"   : 17.4,
      "x": {
        "y": "kevin",
        "z": "paul"
    . . .
    }
    { "integer" : -2002,
      "float"   : -1.2
    }
    . . .

    SELECT `integer` FROM dfs.`/Users/drill/input2.json`;

The output shows values not shown in the snippet. You can take a look at all the values in the input2.json file.

    +------------+
    |  integer   |
    +------------+
    | 2010       |
    | -2002      |
    | 2001       |
    | 6005       |
    +------------+
    4 rows selected (0.113 seconds)

    SELECT ABS(`integer`) FROM dfs.`/Users/drill/input2.json`;

    +------------+
    |   EXPR$0   |
    +------------+
    | 2010       |
    | 2002       |
    | 2001       |
    | 6005       |
    +------------+
    4 rows selected (0.357 seconds)

### CEIL Example
Get the ceiling of float key values in input2.json. The input2.json file contains these float key values:

* 17.4
* -1.2
* 1.2
* 1.2

        SELECT CEIL(`float`) FROM dfs.`/Users/drill/input2.json`;

        +------------+
        |   EXPR$0   |
        +------------+
        | 18.0       |
        | -1.0       |
        | 2.0        |
        | 2.0        |
        +------------+
        4 rows selected (0.647 seconds)

### FLOOR Example
Get the floor of float key values in input2.json.

    SELECT FLOOR(`float`) FROM dfs.`/Users/drill/input2.json`;

    +------------+
    |   EXPR$0   |
    +------------+
    | 17.0       |
    | -2.0       |
    | 1.0        |
    | 1.0        |
    +------------+
    4 rows selected (0.11 seconds)

### ROUND Examples
Open input2.json and change the first float value from 17.4 to 3.14159. Get values of the float columns in input2.json rounded as follows:

* Rounded to the nearest integer.
* Rounded to the fourth decimal place.

        SELECT ROUND(`float`) FROM dfs.`/Users/drill/input2.json`;

        +------------+
        |   EXPR$0   |
        +------------+
        | 3.0        |
        | -1.0       |
        | 1.0        |
        | 1.0        |
        +------------+
        4 rows selected (0.061 seconds)

        SELECT ROUND(`float`, 4) FROM dfs.`/Users/drill/input2.json`;

        +------------+
        |   EXPR$0   |
        +------------+
        | 3.1416     |
        | -1.2       |
        | 1.2        |
        | 1.2        |
        +------------+
        4 rows selected (0.059 seconds)

### LOG Examples

Get the base 2 log of 64.

    SELECT LOG(2, 64) FROM (VALUES(1));

    +------------+
    |   EXPR$0   |
    +------------+
    | 6.0        |
    +------------+
    1 row selected (0.069 seconds)

Get the common log of 100.

    SELECT LOG10(100) FROM (VALUES(1));

    +------------+
    |   EXPR$0   |
    +------------+
    | 2.0        |
    +------------+
    1 row selected (0.203 seconds)

Get the natural log of 7.5.

    SELECT LOG(7.5) FROM (VALUES(1));

    +---------------------+
    |       EXPR$0        |
    +---------------------+
    | 2.0149030205422647  |
    +---------------------+
    1 row selected (0.139 seconds)

## Trig Functions

Drill supports the following trig functions, which return a FLOAT8 result.

* SIN(x)  
  Sine of angle x in radians

* COS(x)  
  Cosine of angle x in radians

* TAN(x)  
  Tangent of angle x in radians

* ASIN(x)  
  Inverse sine of angle x in radians

* ACOS(x)  
  Inverse cosine of angle x in radians

* ATAN(x)  
  Inverse tangent of angle x in radians

* SINH()  
  Hyperbolic sine of hyperbolic angle x in radians

* COSH()  
  Hyperbolic cosine of hyperbolic angle x in radians

* TANH()  
  Hyperbolic tangent of hyperbolic angle x in radians

### Trig Examples

Find the sine and tangent of a 45 degree angle. First convert degrees to radians for use in the SIN() function.

    SELECT RADIANS(30) AS Degrees FROM (VALUES(1));

    +------------+
    |  Degrees   |
    +------------+
    | 0.7853981633974483 |
    +------------+
    1 row selected (0.045 seconds)

    SELECT SIN(0.7853981633974483) AS `Sine of 30 degrees` FROM (VALUES(1));

    +-----------------------+
    |  Sine of 45 degrees   |
    +-----------------------+
    |  0.7071067811865475   |
    +-----------------------+
    1 row selected (0.059 seconds)

    SELECT TAN(0.7853981633974483) AS `Tangent of 30 degrees` from (VALUES(1));

    +-----------------------+
    | Tangent of 45 degrees |
    +-----------------------+
    | 0.9999999999999999    |
    +-----------------------+

