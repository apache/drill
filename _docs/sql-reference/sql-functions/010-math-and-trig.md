---
title: "Math and Trig"
parent: "SQL Functions"
---
Drill supports the math functions shown in the following table of math functions plus trig functions listed at the end of this section. Most math functions and all trig functions take these input types:

* INTEGER
* BIGINT
* FLOAT
* DOUBLE
* SMALLINT*

\* Not supported in this release.

Exceptions are the LSHIFT and RSHIFT functions, which take all types except FLOAT and DOUBLE types. DEGREES, EXP, RADIANS, and the multiple LOG functions take the input types in this list plus the DECIMAL type:

## Table of Math Functions

<table>
  <tr>
    <th>Function</th>
    <th>Return Type</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>ABS(x)</td>
    <td>Same as input</td>
    <td>Returns the absolute value of the input argument x.</td>
  </tr>
  <tr>
    <td>CBRT(x)</td>
    <td>FLOAT8</td>
    <td>Returns the cubic root of x.</td>
  </tr>
  <tr>
    <td>CEIL(x)</td>
    <td>Same as input</td>
    <td>Returns the smallest integer not less than x.</td>
  </tr>
  <tr>
    <td>CEILING(x)</td>
    <td>Same as input</td>
    <td>Same as CEIL.</td>
  </tr>
  <tr>
    <td>DEGREES(x)</td>
    <td>FLOAT8</td>
    <td>Converts x radians to degrees.</td>
  </tr>
  <tr>
    <td>E()</td>
    <td>FLOAT8</td>
    <td>Returns 2.718281828459045.</td>
  </tr>
  <tr>
    <td>EXP(x)</td>
    <td>FLOAT8</td>
    <td>Returns e (Euler's number) to the power of x.</td>
  </tr>
  <tr>
    <td>FLOOR(x)</td>
    <td>Same as input</td>
    <td>Returns the largest integer not greater than x.</td>
  </tr>
  <tr>
    <td>LOG(x)</td>
    <td>FLOAT8</td>
    <td>Returns the natural log (base e) of x.</td>
  </tr>
  <tr>
    <td>LOG(x, y)</td>
    <td>FLOAT8</td>
    <td>Returns log base x to the y power.</td>
  </tr>
  <tr>
    <td>LOG10(x)</td>
    <td>FLOAT8</td>
    <td>Returns the common log of x.</td>
  </tr>
  <tr>
    <td>LSHIFT(x, y)</td>
    <td>Same as input</td>
    <td>Shifts the binary x by y times to the left.</td>
  </tr>
  <tr>
    <td>MOD(x, y)</td>
    <td>FLOAT8</td>
    <td>Returns the remainder of x divided by y. Requires a cast to DECIMAL for consistent results when x and y are FLOAT or DOUBLE.</td>
  </tr>
  <tr>
    <td>NEGATIVE(x)</td>
    <td>Same as input</td>
    <td>Returns x as a negative number.</td>
  </tr>
  <tr>
    <td>PI</td>
    <td>FLOAT8</td>
    <td>Returns pi.</td>
  </tr>
  <tr>
    <td>POW(x, y)</td>
    <td>FLOAT8</td>
    <td>Returns the value of x to the y power.</td>
  </tr>
  <tr>
    <td>RADIANS</td>
    <td>FLOAT8</td>
    <td>Converts x degress to radians.</td>
  </tr>
  <tr>
    <td>RAND</td>
    <td>FLOAT8</td>
    <td>Returns a random number from 0-1.</td>
  </tr>
  <tr>
    <td>ROUND(x)</td>
    <td>Same as input</td>
    <td>Rounds to the nearest integer.</td>
  </tr>
  <tr>
    <td>ROUND(x, y)</td>
    <td>DECIMAL</td>
    <td>Rounds x to s decimal places.</td>
  </tr>
  <tr>
    <td>RSHIFT(x, y)</td>
    <td>Same as input</td>
    <td>Shifts the binary x by y times to the right.</td>
  </tr>
  <tr>
    <td>SIGN(x)</td>
    <td>INT</td>
    <td>Returns the sign of x.</td>
  </tr>
  <tr>
    <td>SQRT(x)</td>
    <td>Same as input</td>
    <td>Returns the square root of x.</td>
  </tr>
  <tr>
    <td>TRUNC(x, y)</td>
    <td>Same as input</td>
    <td>Truncates x to y decimal places. Specifying y is optional. Default is 1.</td>
  </tr>
  <tr>
    <td>TRUNC(x, y)</td>
    <td>DECIMAL</td>
    <td>Truncates x to y decimal places.</td>
  </tr>
</table>

## Math Function Examples

Examples in this section use the `input2.json` file. Download the `input2.json` file from the [Drill source code](https://github.com/apache/drill/tree/master/exec/java-exec/src/test/resources/jsoninput) page. 

You need to use a FROM clause in Drill queries. In addition to using `input2.json`, examples in this documentation often use `FROM sys.version` in the query for example purposes.

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

    SELECT LOG(2, 64) FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | 6.0        |
    +------------+
    1 row selected (0.069 seconds)

Get the common log of 100.

    SELECT LOG10(100) FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | 2.0        |
    +------------+
    1 row selected (0.203 seconds)

Get the natural log of 7.5.

    SELECT LOG(7.5) FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | 2.0149030205422647 |
    +------------+
    1 row selected (0.063 seconds)

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

    SELECT RADIANS(30) AS Degrees FROM sys.version;

    +------------+
    |  Degrees   |
    +------------+
    | 0.7853981633974483 |
    +------------+
    1 row selected (0.045 seconds)

    SELECT SIN(0.7853981633974483) AS `Sine of 30 degrees` FROM sys.version;

    +-----------------------+
    |  Sine of 45 degrees   |
    +-----------------------+
    |  0.7071067811865475   |
    +-----------------------+
    1 row selected (0.059 seconds)

    SELECT TAN(0.7853981633974483) AS `Tangent of 30 degrees` from sys.version;

    +-----------------------+
    | Tangent of 45 degrees |
    +-----------------------+
    | 0.9999999999999999    |
    +-----------------------+

