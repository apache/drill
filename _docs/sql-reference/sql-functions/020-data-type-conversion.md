---
title: "Data Type Conversion"
date: 2018-12-20
parent: "SQL Functions"
---
Drill supports the following functions for casting and converting data types:

* [CAST]({{ site.baseurl }}/docs/data-type-conversion/#cast)
* [CONVERT_TO and CONVERT_FROM]({{ site.baseurl }}/docs/data-type-conversion/#convert_to-and-convert_from)
* [STRING_BINARY]({{ site.baseurl }}/docs/data-type-conversion/#string_binary-function) and [BINARY_STRING]({{ site.baseurl }}/docs/data-type-conversion/#binary_string-function)
* [Other Data Type Conversions]({{ site.baseurl }}/docs/data-type-conversion/#other-data-type-conversions)  

**Note:** Starting in Drill 1.15, all cast and data type conversion functions return an empty string ('') as null when the `drill.exec.functions.cast_empty_string_to_null` option is enabled, for example:  

	SELECT CAST('' AS DATE) FROM (VALUES(1));
	+---------+
	| EXPR$0  |
	+---------+
	| null    |
	+---------+  

Prior to 1.15, casting an empty string to null worked only for numeric types; in Drill 1.15 and later casting an empty string to null also works for DATE, TIME, TIMESTAMP, INTERVAL YEAR, INTERVAL MONTH, and INTERVAL DAY data types. You do not have to use the CASE statement to cast empty strings to null.


## CAST

The CAST function converts an entity, such as an expression that evaluates to a single value, from one type to another.

### CAST Syntax

    CAST (<expression> AS <data type>)

*expression*

A combination of one or more values, operators, and SQL functions that evaluate to a value

*data type*

The target data type, such as INTEGER or DATE, to which to cast the expression

### CAST Usage Notes

Use CONVERT_TO and CONVERT_FROM instead of the CAST function for converting binary data types.

See the following tables for information about the data types to use for casting:

* [CONVERT_TO and CONVERT_FROM Data Types]({{ site.baseurl }}/docs/supported-data-types/#data-types-for-convert_to-and-convert_from-functions)
* [Supported Data Types for Casting]({{ site.baseurl }}/docs/supported-data-types)
* [Explicit Type Casting Maps]({{ site.baseurl }}/docs/supported-data-types/#explicit-type-casting-maps)


## Data Type Conversion Examples

The following examples show how to cast a string to a number, a number to a string, and one type of number to another.

### Casting a Character String to a Number  
You can cast strings or numeric values to decimals, even if they contain decimal points. In cases where a value has a scale and precision greater than the scale and precision specified in the query, the value is rounded to fit the specified scale and precision.

You cannot cast a character string that includes a decimal point to an INT or BIGINT. For example, if you have "1200.50" in a JSON file, attempting to select and cast the string to an INT fails. As a workaround, cast to a FLOAT or DOUBLE type, and then cast to an INT, assuming you want to lose digits to the right of the decimal point.  


The following example shows how to cast a character to a DECIMAL having two decimal places.

    SELECT CAST('1' as DECIMAL(28, 2)) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 1.00       |
    +------------+

### Casting a Number to a Character String
The first example shows Drill casting a number to a VARCHAR having a length of 3 bytes: The result is a 3-character string, 456. Drill supports the CHAR and CHARACTER VARYING alias.

    SELECT CAST(456 as VARCHAR(3)) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 456        |
    +------------+
    1 row selected (0.08 seconds)

    SELECT CAST(456 as CHAR(3)) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 456        |
    +------------+
    1 row selected (0.093 seconds)

### Casting from One Type of Number to Another

Cast an integer to a decimal.

    SELECT CAST(-2147483648 AS DECIMAL(28,8)) FROM (VALUES(1));
    +-----------------+
    |     EXPR$0      |
    +-----------------+
    | -2.147483648E9  |
    +-----------------+  



### Casting Intervals

To cast interval data to interval types you can query from a data source such as JSON, for example, use the following syntax, respectively:

    CAST (column_name AS INTERVAL DAY)
    CAST (column_name AS INTERVAL YEAR)
    CAST (column_name AS INTERVAL SECOND)

For example, a JSON file named `intervals.json` contains the following objects:

    { "INTERVALYEAR_col":"P1Y", "INTERVALDAY_col":"P1D", "INTERVAL_col":"P1Y1M1DT1H1M" }
    { "INTERVALYEAR_col":"P2Y", "INTERVALDAY_col":"P2D", "INTERVAL_col":"P2Y2M2DT2H2M" }
    { "INTERVALYEAR_col":"P3Y", "INTERVALDAY_col":"P3D", "INTERVAL_col":"P3Y3M3DT3H3M" }

Create a table in Parquet from the interval data in the `intervals.json` file.

1. Set the storage format to Parquet.

        ALTER SESSION SET `store.format` = 'parquet';

        +-------+------------------------+
        |  ok   |        summary         |
        +-------+------------------------+
        | true  | store.format updated.  |
        +-------+------------------------+
        1 row selected (0.072 seconds)

2. Use a CTAS statement to cast text from a JSON file to year and day intervals and to write the data to a Parquet table:

        CREATE TABLE dfs.tmp.parquet_intervals AS 
        (SELECT CAST( INTERVALYEAR_col as INTERVAL YEAR) INTERVALYEAR_col, 
                CAST( INTERVALDAY_col as INTERVAL DAY) INTERVALDAY_col,
                CAST( INTERVAL_col as INTERVAL SECOND) INTERVAL_col 
        FROM dfs.`/Users/drill/intervals.json`);

3. Take a look at what Drill wrote to the Parquet file:

        SELECT * FROM dfs.`tmp`.parquet_intervals;
        +-------------------+------------------+---------------+
        | INTERVALYEAR_col  | INTERVALDAY_col  | INTERVAL_col  |
        +-------------------+------------------+---------------+
        | P12M              | P1D              | P1DT3660S     |
        | P24M              | P2D              | P2DT7320S     |
        | P36M              | P3D              | P3DT10980S    |
        +-------------------+------------------+---------------+
        3 rows selected (0.082 seconds)

Because you cast the INTERVAL_col to INTERVAL SECOND, Drill returns the interval data representing the year, month, day, hour, minute, and second. 

## CONVERT_TO and CONVERT_FROM

The CONVERT_TO and CONVERT_FROM functions convert binary data to/from Drill internal types based on the little or big endian encoding of the data.

### CONVERT_TO and CONVERT_FROM Syntax  

    CONVERT_TO (column, type)

    CONVERT_FROM(column, type)

*column* is the name of a column Drill reads.

*type* is one of the encoding types listed in the [CONVERT_TO/FROM data types]({{ site.baseurl }}/docs/supported-data-types/#data-types-for-convert_to-and-convert_from-functions) table. 


### CONVERT_TO and CONVERT_FROM Usage Notes  


- CONVERT_FROM and CONVERT_TO methods transform a known binary representation/encoding to a Drill internal format. Use CONVERT_TO and CONVERT_FROM instead of the CAST function for converting binary data types. CONVERT_TO/FROM functions work for data in a binary representation and are more efficient to use than CAST. 


- Drill can optimize scans on HBase tables when you use the \*\_BE encoded types shown in section  ["Data Types for CONVERT_TO and CONVERT_FROM Functions"]({{ site.baseurl }}/docs/supported-data-types/#data-types-for-convert_to-and-convert_from-functions) on big endian-encoded data. You need to use the HBase storage plugin and query data as described in ["Querying Hbase"]({{ site.baseurl }}/docs/querying-hbase). To write Parquet binary data, convert SQL data *to* binary data and store the data in a Parquet table while creating a table as a selection (CTAS).  


- CONVERT_TO also converts an SQL data type to complex types, including HBase byte arrays, JSON and Parquet arrays, and maps. CONVERT_FROM converts from complex types, including HBase arrays, JSON and Parquet arrays and maps to an SQL data type. 


- You can use [STRING_BINARY]({{ site.baseurl }}/docs/data-type-conversion/#string_binary-function) and [BINARY_STRING]({{ site.baseurl }}/docs/data-type-conversion/#binary_string-function) custom Drill functions with CONVERT_TO and CONVERT_FROM to get meaningful results.  



- Drill 1.13 and later supports [NaN and Infinity values as numeric data types]({{site.baseurl}}/docs/json-data-model/). 
You can use the convert_to and convert_from functions in queries on JSON data with NaN and Infinity values, as shown in the following query examples:  
 
            select convert_fromJSON('{"num": 55, "nan": NaN, "inf": -Infinity}'); 
            select convert_fromJSON(jsonColumn) from mysql.someTable;  
            select string_binary(convert_toJSON(convert_fromJSON(jsonColumn) from mysql.someTable;


### Conversion of Data Types Examples

This example shows how to use the CONVERT_FROM function to convert HBase data to a SQL type. The example summarizes and continues the ["Query HBase"]({{ site.baseurl }}/docs/querying-hbase) example. The ["Query HBase"]({{ site.baseurl }}/docs/querying-hbase) example stores the following data in the students table on the Drill Sandbox:  

    USE maprdb;

    SELECT * FROM students;
        
    +-------------+---------------------+---------------------------------------------------------------------------+
    |   row_key   |  account            |                               address                                     |
    +-------------+---------------------+---------------------------------------------------------------------------+
    | [B@e6d9eb7  | {"name":"QWxpY2U="} | {"state":"Q0E=","street":"MTIzIEJhbGxtZXIgQXY=","zipcode":"MTIzNDU="}     |
    | [B@2823a2b4 | {"name":"Qm9i"}     | {"state":"Q0E=","street":"MSBJbmZpbml0ZSBMb29w","zipcode":"MTIzNDU="}     |
    | [B@3b8eec02 | {"name":"RnJhbms="} | {"state":"Q0E=","street":"NDM1IFdhbGtlciBDdA==","zipcode":"MTIzNDU="}     |
    | [B@242895da | {"name":"TWFyeQ=="} | {"state":"Q0E=","street":"NTYgU291dGhlcm4gUGt3eQ==","zipcode":"MTIzNDU="} |
    +-------------+---------------------+---------------------------------------------------------------------------+
    4 rows selected (1.335 seconds)

You use the CONVERT_FROM function to decode the binary data, selecting a data type to use from the [list of supported types]({{ site.baseurl }}/docs/supported-data-types/#data-types-for-convert_to-and-convert_from-functions). JSON supports strings. To convert bytes to strings, use the UTF8 type:

    SELECT CONVERT_FROM(row_key, 'UTF8') AS studentid, 
           CONVERT_FROM(students.account.name, 'UTF8') AS name, 
           CONVERT_FROM(students.address.state, 'UTF8') AS state, 
           CONVERT_FROM(students.address.street, 'UTF8') AS street, 
           CONVERT_FROM(students.address.zipcode, 'UTF8') AS zipcode FROM students;

    +------------+------------+------------+------------------+------------+
    | studentid  |    name    |   state    |      street      |  zipcode   |
    +------------+------------+------------+------------------+------------+
    | student1   | Alice      | CA         | 123 Ballmer Av   | 12345      |
    | student2   | Bob        | CA         | 1 Infinite Loop  | 12345      |
    | student3   | Frank      | CA         | 435 Walker Ct    | 12345      |
    | student4   | Mary       | CA         | 56 Southern Pkwy | 12345      |
    +------------+------------+------------+------------------+------------+
    4 rows selected (0.504 seconds)

This example converts VARCHAR data to a JSON map:

    SELECT CONVERT_FROM('{x:100, y:215.6}' ,'JSON') AS MYCOL FROM (VALUES(1));
    +----------------------+
    |        MYCOL         |
    +----------------------+
    | {"x":100,"y":215.6}  |
    +----------------------+
    1 row selected (0.163 seconds)

This example uses a list of BIGINT data as input and returns a repeated list of vectors:

    SELECT CONVERT_FROM('[ [1, 2], [3, 4], [5]]' ,'JSON') AS MYCOL1 FROM (VALUES(1));
    +------------+
    |   mycol1   |
    +------------+
    | [[1,2],[3,4],[5]] |
    +------------+
    1 row selected (0.054 seconds)

This example uses a map as input to return a repeated list vector (JSON).

    SELECT CONVERT_FROM('[{a : 100, b: 200}, {a:300, b: 400}]' ,'JSON') AS MYCOL1  FROM (VALUES(1));
    +--------------------+
    |       MYCOL1       |
    +--------------------+
    | [[1,2],[3,4],[5]]  |
    +--------------------+
    1 row selected (0.141 seconds)

### Set Up a Storage Plugin for Working with HBase

This example assumes you are working in the Drill Sandbox. You modify the `dfs` storage plugin slightly and use that plugin for this example.

1. Copy/paste the `dfs` storage plugin definition to a newly created plugin called myplugin.

2. Change the root location to "/mapr/demo.mapr.com/tables". After this change, you can read a table in the `tables` directory. You can write a converted version of the table in the `tmp` directory because the writable property is true.

        {
          "type": "file",
          "enabled": true,
          "connection": "maprfs:///",
          "workspaces": {
            "root": {
              "location": "/mapr/demo.mapr.com/tables",
              "writable": true,
              "defaultInputFormat": null
            },
         
            . . .

            "tmp": {
              "location": "/tmp",
              "writable": true,
              "defaultInputFormat": null
            }

            . . .
         
          "formats": {
            . . .
            "maprdb": {
              "type": "maprdb"
            }
          }
        }

### Convert the Binary HBase Students Table to JSON Data

First, you set the storage format to JSON. Next, you use the CREATE TABLE AS (CTAS) statement to convert from a selected file of a different format, HBase in this example, to the storage format. You then convert the JSON file to Parquet using a similar procedure. Set the storage format to Parquet, and use a CTAS statement to convert to Parquet from JSON. In each case, you [select UTF8]({{ site.baseurl }}/docs/supported-data-types/#data-types-for-convert_to-and-convert_from-functions) as the file format because the data you are converting from and then to consists of strings.

1. Start Drill on the Drill Sandbox and set the default storage format from Parquet to JSON.

        ALTER SESSION SET `store.format`='json';

2. Use CONVERT_FROM queries to convert the binary data in the HBase students table to JSON, and store the JSON data in a file. You select a data type to use from the supported. JSON supports strings. To convert binary to strings, use the UTF8 type.

        CREATE TABLE tmp.`to_json` AS SELECT 
            CONVERT_FROM(row_key, 'UTF8') AS `studentid`, 
            CONVERT_FROM(students.account.name, 'UTF8') AS name, 
            CONVERT_FROM(students.address.state, 'UTF8') AS state, 
            CONVERT_FROM(students.address.street, 'UTF8') AS street, 
            CONVERT_FROM(students.address.zipcode, 'UTF8') AS zipcode 
        FROM root.`students`;

        +------------+---------------------------+
        |  Fragment  | Number of records written |
        +------------+---------------------------+
        | 0_0        | 4                         |
        +------------+---------------------------+
        1 row selected (0.41 seconds)
4. Navigate to the output. 

        cd /mapr/demo.mapr.com/tmp/to_json
        ls
   Output is:

        0_0_0.json

5. Take a look at the output of `to_json`:

        {
          "studentid" : "student1",
          "name" : "Alice",
          "state" : "CA",
          "street" : "123 Ballmer Av",
          "zipcode" : "12345"
        } {
          "studentid" : "student2",
          "name" : "Bob",
          "state" : "CA",
          "street" : "1 Infinite Loop",
          "zipcode" : "12345"
        } {
          "studentid" : "student3",
          "name" : "Frank",
          "state" : "CA",
          "street" : "435 Walker Ct",
          "zipcode" : "12345"
        } {
          "studentid" : "student4",
          "name" : "Mary",
          "state" : "CA",
          "street" : "56 Southern Pkwy",
          "zipcode" : "12345"
        }

6. Set up Drill to store data in Parquet format.

        ALTER SESSION SET `store.format`='parquet';
        +-------+------------------------+
        |  ok   |        summary         |
        +-------+------------------------+
        | true  | store.format updated.  |
        +-------+------------------------+
        1 row selected (0.07 seconds)

7. Use CONVERT_TO to convert the JSON data to a binary format in the Parquet file.

        CREATE TABLE tmp.`json2parquet` AS SELECT 
            CONVERT_TO(studentid, 'UTF8') AS id, 
            CONVERT_TO(name, 'UTF8') AS name, 
            CONVERT_TO(state, 'UTF8') AS state, 
            CONVERT_TO(street, 'UTF8') AS street, 
            CONVERT_TO(zipcode, 'UTF8') AS zip 
        FROM tmp.`to_json`;

        +------------+---------------------------+
        |  Fragment  | Number of records written |
        +------------+---------------------------+
        | 0_0        | 4                         |
        +------------+---------------------------+
        1 row selected (0.414 seconds)
8. Take a look at the meaningless output from sqlline:

        SELECT * FROM tmp.`json2parquet`;
        +-------------+-------------+-------------+-------------+-------------+
        |      id     |    name     |    state    |   street    |     zip     |
        +-------------+-------------+-------------+-------------+-------------+
        | [B@224388b2 | [B@7fc36fb0 | [B@77d9cd57 | [B@7c384839 | [B@530dd5e5 |
        | [B@3155d7fc | [B@7ad6fab1 | [B@37e4b978 | [B@94c91f3  | [B@201ed4a  |
        | [B@4fb2c078 | [B@607a2f28 | [B@75ae1c93 | [B@79d63340 | [B@5dbeed3d |
        | [B@2fcfec74 | [B@7baccc31 | [B@d91e466  | [B@6529eb7f | [B@232412bc |
        +-------------+-------------+-------------+-------------+-------------+
        4 rows selected (0.12 seconds)

9. Use CONVERT_FROM to read the Parquet data:

        SELECT CONVERT_FROM(id, 'UTF8') AS id, 
               CONVERT_FROM(name, 'UTF8') AS name, 
               CONVERT_FROM(state, 'UTF8') AS state, 
               CONVERT_FROM(street, 'UTF8') AS address, 
               CONVERT_FROM(zip, 'UTF8') AS zip 
        FROM tmp.`json2parquet2`;

        +------------+------------+------------+------------------+------------+
        |     id     |    name    |   state    |  address         |    zip     |
        +------------+------------+------------+------------------+------------+
        | student1   | Alice      | CA         | 123 Ballmer Av   | 12345      |
        | student2   | Bob        | CA         | 1 Infinite Loop  | 12345      |
        | student3   | Frank      | CA         | 435 Walker Ct    | 12345      |
        | student4   | Mary       | CA         | 56 Southern Pkwy | 12345      |
        +------------+------------+------------+------------------+------------+
        4 rows selected (0.182 seconds)

## STRING_BINARY function

Prints the bytes that are printable, and prints a hexadecimal
representation for bytes that are not printable. This function is modeled after the hbase utilities.

### STRING_BINARY Syntax

    STRING_BINARY(expression)

*expression* is a byte array, such as {(byte)0xca, (byte)0xfe, (byte)0xba, (byte)0xbe}

You can use this function with CONVERT_TO when you want to test the effects of a conversion.

### STRING_BINARY Examples

```
SELECT
  STRING_BINARY(CONVERT_TO(1, 'INT')) as i,
  STRING_BINARY(CONVERT_TO(1, 'INT_BE')) as i_be,
  STRING_BINARY(CONVERT_TO(1, 'BIGINT')) as l,
  STRING_BINARY(CONVERT_TO(1, 'BIGINT')) as l_be,
  STRING_BINARY(CONVERT_TO(1, 'INT_HADOOPV')) as l_be
FROM (VALUES (1));
```
Output is:

```
+-------------------+-------------------+-----------------------------------+-----------------------------------+--------+
|         i         |       i_be        |                 l                 |               l_be                | l_be0  |
+-------------------+-------------------+-----------------------------------+-----------------------------------+--------+
| \x01\x00\x00\x00  | \x00\x00\x00\x01  | \x01\x00\x00\x00\x00\x00\x00\x00  | \x01\x00\x00\x00\x00\x00\x00\x00  | \x01   |
+-------------------+-------------------+-----------------------------------+-----------------------------------+--------+
1 row selected (0.323 seconds)
```
Encode 'hello' in UTF-8 and UTF-16 VARBINARY encoding and return the results as a VARCHAR.

```
SELECT
  STRING_BINARY(CONVERT_TO('hello', 'UTF8')) u8,
  STRING_BINARY(CONVERT_TO('hello', 'UTF16')) u16
FROM (VALUES (1));
```

```
+--------+------------------------------------+
|   u8   |                u16                 |
+--------+------------------------------------+
| hello  | \xFE\xFF\x00h\x00e\x00l\x00l\x00o  |
+--------+------------------------------------+
1 row selected (0.168 seconds)
```

## BINARY_STRING function

Converts a string that is the hexadecimal encoding of a sequence of bytes into a VARBINARY value. 

### BINARY_STRING Syntax

    BINARY_STRING(expression)

*expression* is a hexadecimal string, such as `"\xca\xfe\xba\xbe"`.

This function returns a byte array, such as {(byte)0xca, (byte)0xfe, (byte)0xba, (byte)0xbe}. 

### BINARY_STRING Example

Converts a VARBINARY type into a hexadecimal-encoded string.

### BINARY_STRING Syntax

    BINARY_STRING(expression)

*expression* is a byte array, such as {(byte)0xca, (byte)0xfe, (byte)0xba, (byte)0xbe}.

This function returns a hexadecimal-encoded string, such as `"\xca\xfe\xba\xbe"`. You can use this function with CONVERT_FROM/TO for meaningful results.

### BINARY_STRING Examples

Decode the hexadecimal string 000000C8 expressed in four octets `\x00\x00\x00\xC8` into an integer equivalent using big endian encoding. 

```
SELECT CONVERT_FROM(BINARY_STRING('\x00\x00\x00\xC8'), 'INT_BE') AS cnvrt
FROM (VALUES (1));
```

Output is:

```
+--------+
| cnvrt  |
+--------+
| 200    |
+--------+
1 row selected (0.133 seconds)
```

Decode the same hexadecimal string into an integer using little endian encoding.

```
SELECT CONVERT_FROM(BINARY_STRING('\x00\x00\x00\xC8'), 'INT') AS cnvrt FROM (VALUES (1));
```

Output is:

```
+-------------+
|    cnvrt    |
+-------------+
| -939524096  |
+-------------+
1 row selected (0.133 seconds)
```

Convert a hexadecimal number BEBAFECA to its decimal equivalent and then to its VARBINARY equivalent, which is how the value is represented in HBase.

```
SELECT CONVERT_FROM(BINARY_STRING('\xBE\xBA\xFE\xCA'), 'INT_BE') FROM (VALUES (1));

+--------------+
|    EXPR$0    |
+--------------+
| -1095041334  |
+--------------+

SELECT CONVERT_TO(-1095041334, 'INT_BE') FROM (VALUES (1));

+--------------+
|    EXPR$0    |
+--------------+
| [B@10c24faf  |
+--------------+
1 row selected (0.161 seconds)
```

## Other Data Type Conversions
Drill supports the format for date and time literals shown in the following examples:

* 2008-12-15

* 22:55:55.123...

If you have dates and times in other formats, use a data type conversion function to perform the following conversions:

* A TIMESTAMP, DATE, TIME, INTEGER, FLOAT, or DOUBLE to a character string, which is of type VARCHAR
* A character string to a DATE
* A character string to a NUMBER

The following table lists data type formatting functions that you can
use in your Drill queries as described in this section:

| Function                                                                                         | Return Type |
|--------------------------------------------------------------------------------------------------|-------------|
| [TO_CHAR]({{site.baseurl}}/docs/data-type-conversion/#to_char)(expression, format)               | VARCHAR     |
| [TO_DATE]({{site.baseurl}}/docs/data-type-conversion/#to_date)(expression, format)               | DATE        |
| [TO_NUMBER]({{site.baseurl}}/docs/data-type-conversion/#to_number)(VARCHAR, format)              | DECIMAL     |
| [TO_TIMESTAMP]({{site.baseurl}}/docs/data-type-conversion/#to_timestamp)(VARCHAR, format)        | TIMESTAMP   |
| [TO_TIMESTAMP]({{site.baseurl}}/docs/data-type-conversion/#to_timestamp)(DOUBLE)                 | TIMESTAMP   |

### Format Specifiers for Numerical Conversions
Use the following Java format specifiers for converting numbers:

| Symbol     | Location            | Meaning                                                                                                                                                                                              |
|------------|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0          | Number              | Digit                                                                                                                                                                                                |
| #          | Number              | Digit, zero shows as absent                                                                                                                                                                          |
| .          | Number              | Decimal separator or monetary decimal separator                                                                                                                                                      |
| -          | Number              | Minus sign                                                                                                                                                                                           |
| ,          | Number              | Grouping separator                                                                                                                                                                                   |
| E          | Number              | Separates mantissa and exponent in scientific notation. Need not be quoted in prefix or suffix.                                                                                                      |
| ;          | Subpattern boundary | Separates positive and negative subpatterns                                                                                                                                                          |
| %          | Prefix or suffix    | Multiply by 100 and show as percentage                                                                                                                                                               |
| \u2030     | Prefix or suffix    | Multiply by 1000 and show as per mille value                                                                                                                                                         |
| _ (\u00A4) | Prefix or suffix    | Currency sign, replaced by currency symbol. If doubled, replaced by international currency symbol. If present in a pattern, the monetary decimal separator is used instead of the decimal separator. |
| '          | Prefix or suffix    | Used to quote special characters in a prefix or suffix, for example, "'#'#"" formats 123 to ""#123"". To create a single quote itself, use two in a row: "# o''clock".                               |  

### Format Specifiers for Date/Time Conversions

Use the following Joda format specifiers for date/time conversions:

| Symbol | Meaning                                          | Presentation | Examples                           |
|--------|--------------------------------------------------|--------------|------------------------------------|
| G      | era                                              | text         | AD                                 |
| C      | century of era (>=0)                             | number       | 20                                 |
| Y      | year of era (>=0)                                | year         | 1996                               |
| x      | weekyear                                         | year         | 1996                               |
| w      | week of weekyear                                 | number       | 27                                 |
| e      | day of week                                      | number       | 2                                  |
| E      | day of week                                      | text         | Tuesday; Tue                       |
| y      | year                                             | year         | 1996                               |
| D      | day of year                                      | number       | 189                                |
| M      | month of year                                    | month        | July; Jul; 07                      |
| d      | day of month                                     | number       | 10                                 |
| a      | halfday of day                                   | text         | PM                                 |
| K      | hour of halfday (0~11)                           | number       | 0                                  |
| h      | clockhour of halfday (1~12) number               | 12           |                                    |
| H      | hour of day (0~23)                               | number       | 0                                  |
| k      | clockhour of day (1~24)                          | number       | 24                                 |
| m      | minute of hour                                   | number       | 30                                 |
| s      | second of minute                                 | number       | 55                                 |
| S      | fraction of second                               | number       | 978                                |
| z      | time zone                                        | text         | Pacific Standard Time; PST         |
| Z      | time zone offset/id                              | zone         | -0800; -08:00; America/Los_Angeles |
| '      | single quotation mark, escape for text delimiter | literal      |                                    |  

{% include startnote.html %}The Joda format specifiers are case-sensitive.{% include endnote.html %}

For more information about specifying a format, refer to one of the following format specifier documents:

* [Java DecimalFormat class](http://docs.oracle.com/javase/7/docs/api/java/text/DecimalFormat.html) format specifiers 
* [Joda DateTimeFormat class](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) format specifiers

## TO_CHAR

TO_CHAR converts a number, date, time, or timestamp expression to a character string.

### TO_CHAR Syntax

    TO_CHAR (expression, 'format')

*expression* is a INTEGER, FLOAT, DOUBLE, DATE, TIME, or TIMESTAMP expression. 

*'format'* is a format specifier enclosed in single quotation marks that sets a pattern for the output formatting. 

### TO_CHAR Usage Notes

You can use the ‘z’ option to identify the time zone in TO_TIMESTAMP to make sure the timestamp has the timezone in it, as shown in the TO_TIMESTAMP description.

### TO_CHAR Examples

Convert a FLOAT to a character string. The format specifications use a comma to separate thousands and round-off to three decimal places.

    SELECT TO_CHAR(1256.789383, '#,###.###') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 1,256.789  |
    +------------+
    1 row selected (1.767 seconds)

Convert an integer to a character string.

    SELECT TO_CHAR(125677.4567, '#,###.###') FROM (VALUES(1));
    +--------------+
    |    EXPR$0    |
    +--------------+
    | 125,677.457  |
    +--------------+
    1 row selected (0.083 seconds)

Convert a date to a character string.

    SELECT TO_CHAR((CAST('2008-2-23' AS DATE)), 'yyyy-MMM-dd') FROM (VALUES(1));
    +--------------+
    |    EXPR$0    |
    +--------------+
    | 2008-Feb-23  |
    +--------------+
    1 row selected (0.166 seconds)

Convert a time to a string.

    SELECT TO_CHAR(CAST('12:20:30' AS TIME), 'HH mm ss') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 12 20 30   |
    +------------+
    1 row selected (0.07 seconds)


Convert a timestamp to a string.

    SELECT TO_CHAR(CAST('2015-2-23 12:00:00' AS TIMESTAMP), 'yyyy MMM dd HH:mm:ss') FROM (VALUES(1));
    +-----------------------+
    |        EXPR$0         |
    +-----------------------+
    | 2015 Feb 23 12:00:00  |
    +-----------------------+
    1 row selected (0.142 seconds)

## TO_DATE
Converts a character string or a UNIX epoch timestamp to a date.

### TO_DATE Syntax

    TO_DATE (expression [, 'format'])

*expression* is a character string enclosed in single quotation marks or a Unix epoch timestamp in milliseconds, not enclosed in single quotation marks. 

*'format'* is a character string that specifies the format of *expression*. Only use this option when the *expression* is a character string, not a UNIX epoch timestamp. 

### TO_DATE Usage Notes
Specify a format using patterns defined in [Joda DateTimeFormat class](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html). The TO_TIMESTAMP function takes a Unix epoch timestamp. The TO_DATE function takes a UNIX epoch timestamp in milliseconds. The [UNIX_TIMESTAMP]({{site.baseurl}}/docs/date-time-functions-and-arithmetic/#unix_timestamp) function converts a time string to a UNIX timestamp in seconds. 

To compare dates in the WHERE clause, use TO_DATE on the value in the date column and in the comparison value. For example:

    SELECT <fields> FROM <plugin> WHERE TO_DATE(<field>, <format>) < TO_DATE (<value>, <format>);

For example:

    SELECT TO_DATE(`date`, 'yyyy-MM-dd') FROM `sample.json`;

    +------------+
    |   EXPR$0   |
    +------------+
    | 2013-07-26 |
    | 2013-05-16 |
    | 2013-06-09 |
    | 2013-07-19 |
    | 2013-07-21 |
    +------------+
    5 rows selected (0.134 seconds)

    SELECT TO_DATE(`date`, 'yyyy-MM-dd') FROM `sample.json` WHERE TO_DATE(`date`, 'yyyy-MM-dd') < TO_DATE('2013-07-20', 'yyyy-MM-dd');
    
    +------------+
    |   EXPR$0   |
    +------------+
    | 2013-05-16 |
    | 2013-06-09 |
    | 2013-07-19 |
    +------------+
    3 rows selected (0.177 seconds)

### TO_DATE Examples
The first example converts a character string to a date. The second example extracts the year to verify that Drill recognizes the date as a date type. 

    SELECT TO_DATE('2015-FEB-23', 'yyyy-MMM-dd') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-02-23 |
    +------------+
    1 row selected (0.077 seconds)

    SELECT EXTRACT(year from mydate) `extracted year` FROM (SELECT TO_DATE('2015-FEB-23', 'yyyy-MMM-dd') AS mydate FROM (VALUES(1)));

    +------------+
    |   myyear   |
    +------------+
    | 2015       |
    +------------+
    1 row selected (0.128 seconds)

The following example converts a UNIX epoch timestamp to a date.

    SELECT TO_DATE(1427849046000) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-04-01 |
    +------------+
    1 row selected (0.082 seconds)

## TO_NUMBER

TO_NUMBER converts a character string to a formatted number using a format specification.

### Syntax

    TO_NUMBER ('string', 'format')

*'string'* is a character string enclosed in single quotation marks. 

*'format'* is one or more [Java DecimalFormat class](http://docs.oracle.com/javase/7/docs/api/java/text/DecimalFormat.html) specifiers enclosed in single quotation marks that set a pattern for the output formatting.


### TO_NUMBER Usage Notes
The data type of the output of TO_NUMBER is a numeric. You can use the following [Java DecimalFormat class](http://docs.oracle.com/javase/7/docs/api/java/text/DecimalFormat.html) specifiers to set the output formatting. 

* #  
  Digit placeholder. 

* 0  
  Digit placeholder. If a value has a digit in the position where the zero '0' appears in the format string, that digit appears in the output; otherwise, a '0' appears in that position in the output.

* .  
  Decimal point. Make the first '.' character in the format string the location of the decimal separator in the value; ignore any additional '.' characters.

* ,  
  Comma grouping separator. 

* E
  Exponent. Separates mantissa and exponent in scientific notation. 

### TO_NUMBER Examples

    SELECT TO_NUMBER('987,966', '######') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 987.0      |
    +------------+

    SELECT TO_NUMBER('987.966', '###.###') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 987.966    |
    +------------+
    1 row selected (0.063 seconds)

    SELECT TO_NUMBER('12345', '##0.##E0') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 12345.0    |
    +------------+
    1 row selected (0.069 seconds)

## TO_TIME
Converts a character string to a time.

### TO_TIME Syntax

    TO_TIME (expression [, 'format'])

*expression* is a character string enclosed in single quotation marks or milliseconds, not enclosed in single quotation marks. 

*'format'* is a format specifier enclosed in single quotation marks that sets a pattern for the output formatting. Use this option only when the expression is a character string, not milliseconds. 

## TO_TIME Usage Notes
Specify a format using patterns defined in [Joda DateTimeFormat class](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html).

### TO_TIME Examples

    SELECT TO_TIME('12:20:30', 'HH:mm:ss') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 12:20:30   |
    +------------+
    1 row selected (0.067 seconds)

Convert 828550000 milliseconds (23 hours 55 seconds) to the time.

    SELECT to_time(82855000) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 23:00:55   |
    +------------+
    1 row selected (0.086 seconds)

## TO_TIMESTAMP

### TO_TIMESTAMP Syntax

    TO_TIMESTAMP (expression [, 'format'])

*expression* is a character string enclosed in single quotation marks or a UNIX epoch timestamp, not enclosed in single quotation marks. 

*'format'* is a character string that specifies the format of *expression*. Only use this option when the *expression* is a character string, not a UNIX epoch timestamp. 

### TO_TIMESTAMP Usage Notes
Specify a format using patterns defined in [Joda DateTimeFormat class](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html). The TO_TIMESTAMP function takes a Unix epoch timestamp. The TO_DATE function takes a UNIX epoch timestamp in milliseconds.

### TO_TIMESTAMP Examples

Convert a date to a timestamp. 

    SELECT TO_TIMESTAMP('2008-2-23 12:00:00', 'yyyy-MM-dd HH:mm:ss') FROM (VALUES(1));
    +------------------------+
    |         EXPR$0         |
    +------------------------+
    | 2008-02-23 12:00:00.0  |
    +------------------------+
    1 row selected (0.126 seconds)

Convert Unix Epoch time to a timestamp.

    SELECT TO_TIMESTAMP(1427936330) FROM (VALUES(1));
    +------------------------+
    |         EXPR$0         |
    +------------------------+
    | 2015-04-01 17:58:50.0  |
    +------------------------+
    1 row selected (0.114 seconds)

Convert a UTC date to a timestamp offset from the UTC time zone code.

    SELECT TO_TIMESTAMP('2015-03-30 20:49:59.0 UTC', 'YYYY-MM-dd HH:mm:ss.s z') AS Original, 
           TO_CHAR(TO_TIMESTAMP('2015-03-30 20:49:59.0 UTC', 'YYYY-MM-dd HH:mm:ss.s z'), 'z') AS New_TZ 
    FROM (VALUES(1));

    +------------------------+---------+
    |        Original        | New_TZ  |
    +------------------------+---------+
    | 2015-03-30 20:49:00.0  | UTC     |
    +------------------------+---------+
    1 row selected (0.148 seconds)

## Time Zone Limitation
Currently Drill does not support conversion of a date, time, or timestamp from one time zone to another. Queries of data associated with a time zone can return inconsistent results or an error. For more information, see the ["Understanding Drill's Timestamp and Timezone"](http://www.openkb.info/2015/05/understanding-drills-timestamp-and.html#.VUzhotpVhHw) blog. The Drill time zone is based on the operating system time zone unless you override it. To work around the limitation, configure Drill to use [UTC](http://www.timeanddate.com/time/aboututc.html)-based time, convert your data to UTC timestamps, and perform date/time operation in UTC.  

1. Take a look at the Drill time zone configuration by running the TIMEOFDAY function or by querying the system.options table. This TIMEOFDAY function returns the local date and time with time zone information. 

        SELECT TIMEOFDAY() FROM (VALUES(1));

        +----------------------------------------------+
        |                    EXPR$0                    |
        +----------------------------------------------+
        | 2015-05-17 22:37:29.516 America/Los_Angeles  |
        +----------------------------------------------+
        1 row selected (0.108 seconds)

2. Configure the default time zone format in <drill installation directory>/conf/drill-env.sh by adding `-Duser.timezone=UTC` to DRILL_JAVA_OPTS. For example:

        export DRILL_JAVA_OPTS="-Xms1G -Xmx$DRILL_MAX_HEAP -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G -ea -Duser.timezone=UTC"

3. Restart the Drill shell.

4. Confirm that Drill is now set to UTC:

        SELECT TIMEOFDAY() FROM (VALUES(1));

        +----------------------------------------------+
        |                    EXPR$0                    |
        +----------------------------------------------+
        | 2015-05-17 22:37:57.082 America/Los_Angeles  |
        +----------------------------------------------+
        1 row selected (0.087 seconds)

You can use the ‘z’ option to identify the time zone in TO_TIMESTAMP to make sure the timestamp has the timezone in it. Also, use the ‘z’ option to identify the time zone in a timestamp using the TO_CHAR function. For example:

    SELECT TO_TIMESTAMP('2015-03-30 20:49:59.0 UTC', 'YYYY-MM-dd HH:mm:ss.s z') AS Original, 
           TO_CHAR(TO_TIMESTAMP('2015-03-30 20:49:59.0 UTC', 'YYYY-MM-dd HH:mm:ss.s z'), 'z') AS TimeZone 
           FROM (VALUES(1));

    +------------------------+-----------+
    |        Original        | TimeZone  |
    +------------------------+-----------+
    | 2015-03-30 20:49:00.0  | UTC       |
    +------------------------+-----------+
    1 row selected (0.097 seconds)

<!-- DRILL-448 Support timestamp with time zone -->


<!-- Apache Drill    
Apache DrillDRILL-1141
ISNUMERIC should be implemented as a SQL function
SELECT count(columns[0]) as number FROM dfs.`bla` WHERE ISNUMERIC(columns[0])=1
 -->
