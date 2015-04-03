---
title: "Casting/Converting Data Types"
parent: "SQL Functions"
---
Drill supports the following functions for casting and converting data types:

* [CAST](/docs/data-type-fmt#cast)
* [CONVERT TO/FROM](/docs/data-type-fmt#convert-to-and-convert-from)
* [Other data type conversion functions](/docs/data-type-fmt#other-data-type-conversion-functions)

## CAST

The CAST function converts an entity having a single data value, such as a column name, from one type to another.

### Syntax

cast (<expression> AS <data type>)

*expression*

An entity that evaluates to one or more values, such as a column name or literal

*data type*

The target data type, such as INTEGER or DATE, to which to cast the expression

### Usage Notes

If the SELECT statement includes a WHERE clause that compares a column of an unknown data type, cast both the value of the column and the comparison value in the WHERE clause. For example:

    SELECT c_row, CAST(c_int AS DECIMAL(28,8)) FROM mydata WHERE CAST(c_int AS DECIMAL(28,8)) > -3.0

Do not use the CAST function for converting binary data types to other types. Although CAST works for converting VARBINARY to VARCHAR, CAST does not work in other cases for converting binary data. Use CONVERT_TO and CONVERT_FROM for converting to or from binary data. 

Refer to the following tables for information about the data types to use for casting:

* [Supported Data Types for Casting](/docs/supported-data-types-for-casting)
* [Explicit Type Casting Maps](/docs/explicit-type-casting-maps)


### Examples

The following examples show how to cast a string to a number, a number to a string, and casting from one numerical type to another.

#### Casting a character string to a number
You cannot cast a character string that includes a decimal point to an INT or BIGINT. For example, if you have "1200.50" in a JSON file, attempting to select and cast the string to an INT fails. As a workaround, cast to a float or decimal type, and then to an integer type. 

The following example shows how to cast a character to a DECIMAL having two decimal places.

    SELECT CAST('1' as DECIMAL(28, 2)) FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 1.00       |
    +------------+

#### Casting a number to a character string
The first example shows that Drill uses a default limit of 1 character if you omit the VARCHAR limit: The result is truncated to 1 character.  The second example casts the same number to a VARCHAR having a limit of 3 characters: The result is a 3-character string, 456. The third example shows that you can use CHAR as an alias for VARCHAR. You can also use CHARACTER or CHARACTER VARYING.

    SELECT CAST(456 as VARCHAR) FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 4          |
    +------------+
    1 row selected (0.063 seconds)

    SELECT CAST(456 as VARCHAR(3)) FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 456        |
    +------------+
    1 row selected (0.08 seconds)

    SELECT CAST(456 as CHAR(3)) FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 456        |
    +------------+
    1 row selected (0.093 seconds)

#### Casting from one numerical type to another

Cast an integer to a decimal.

    SELECT CAST(-2147483648 AS DECIMAL(28,8)) FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | -2.147483648E9 |
    +------------+
    1 row selected (0.08 seconds)

### Casting Intervals

To cast INTERVAL data use the following syntax:

    CAST (column_name AS INTERVAL)
    CAST (column_name AS INTERVAL DAY)
    CAST (column_name AS INTERVAL YEAR)

A JSON file contains the following objects:

    { "INTERVALYEAR_col":"P1Y", "INTERVALDAY_col":"P1D", "INTERVAL_col":"P1Y1M1DT1H1M" }
    { "INTERVALYEAR_col":"P2Y", "INTERVALDAY_col":"P2D", "INTERVAL_col":"P2Y2M2DT2H2M" }
    { "INTERVALYEAR_col":"P3Y", "INTERVALDAY_col":"P3D", "INTERVAL_col":"P3Y3M3DT3H3M" }

The following CTAS statement shows how to cast text from a JSON file to INTERVAL data types in a Parquet table:

    CREATE TABLE dfs.tmp.parquet_intervals AS 
    (SELECT cast (INTERVAL_col as interval),
           cast( INTERVALYEAR_col as interval year) INTERVALYEAR_col, 
           cast( INTERVALDAY_col as interval day) INTERVALDAY_col 
    FROM `/user/root/intervals.json`);

<!-- Text and include output -->

## CONVERT_TO and CONVERT_FROM

The CONVERT_TO and CONVERT_FROM functions encode and decode
data, respectively.

## Syntax  

CONVERT_TO (type, expression)

You can use CONVERT functions to convert any compatible data type to any other type. HBase stores data as encoded byte arrays (VARBINARY data). To query HBase data in Drill, convert every column of an HBase table to/from byte arrays from/to an SQL data type that Drill supports when writing/reading data.  The CONVERT fumctions are more efficient than CAST when your data sources return binary data. 

## Usage Notes
Use the CONVERT_TO function to change the data type to bytes when sending data back to HBase from a Drill query. CONVERT_TO converts an SQL data type to complex types, including Hbase byte arrays, JSON and Parquet arrays and maps. CONVERT_FROM converts from complex types, including Hbase byte arrays, JSON and Parquet arrays and maps to an SQL data type. 

### Example

A common use case for CONVERT_FROM is to convert complex data embedded in
a HBase column to a readable type. The following example converts VARBINARY data in col1 from HBase or MapR-DB table to JSON data. 

    SELECT CONVERT_FROM(col1, 'JSON') 
    FROM hbase.table1
    ...


## Other Data Type Conversions
In addition to the CAST, CONVERT_TO, and CONVERT_FROM functions, Drill supports data type conversion functions to perform the following conversions:

* A timestamp, integer, decimal, or double to a character string.
* A character string to a date
* A character string to a number

## Time Zone Limitation
Currently Drill does not support conversion of a date, time, or timestamp from one time zone to another. 

The workaround is to configure Drill to use [UTC](http://www.timeanddate.com/time/aboututc.html)-based time, convert your data to UTC timestamps, and perform date/time operation in UTC.  

1. Take a look at the Drill time zone configuration by running the TIMEOFDAY function. This function returns the local date and time with time zone information.

    0: jdbc:drill:zk=local> select timeofday() from sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-04-02 15:01:31.114 America/Los_Angeles |
    +------------+
    1 row selected (1.199 seconds)

2. Configure the default time zone format in <drill installation directory>/conf/drill-env.sh by adding `-Duser.timezone=UTC` to DRILL_JAVA_OPTS. For example:

    export DRILL_JAVA_OPTS="-Xms1G -Xmx$DRILL_MAX_HEAP -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G -ea -Duser.timezone=UTC"

3. Restart sqlline.

4. Confirm that Drill is now set to UTC:

    SELECT TIMEOFDAY() from sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-04-02 17:05:02.424 UTC |
    +------------+
    1 row selected (1.191 seconds)


<!-- A character string to a timestamp with time zone

A decimal type to a timestamp with time zone -->

### Format Specifiers for Numerical Conversions
Use the following format specifiers for numerical conversions:
<table >
     <tr >
          <th align=left>Symbol
          <th align=left>Location
          <th align=left>Meaning
     <tr valign=top>
          <td><code>0</code>
          <td>Number
          <td>Digit
     <tr >
          <td><code>#</code>
          <td>Number
          <td>Digit, zero shows as absent
     <tr valign=top>
          <td><code>.</code>
          <td>Number
          <td>Decimal separator or monetary decimal separator
     <tr >
          <td><code>-</code>
          <td>Number
          <td>Minus sign
     <tr valign=top>
          <td><code>,</code>
          <td>Number
          <td>Grouping separator
     <tr >
          <td><code>E</code>
          <td>Number
          <td>Separates mantissa and exponent in scientific notation.
              <em>Need not be quoted in prefix or suffix.</em>
     <tr valign=top>
          <td><code>;</code>
          <td>Subpattern boundary
          <td>Separates positive and negative subpatterns
     <tr >
          <td><code>%</code>
          <td>Prefix or suffix
          <td>Multiply by 100 and show as percentage
     <tr valign=top>
          <td><code>&#92;u2030</code>
          <td>Prefix or suffix
          <td>Multiply by 1000 and show as per mille value
     <tr >
          <td><code>&#164;</code> (<code>&#92;u00A4</code>)
          <td>Prefix or suffix
          <td>Currency sign, replaced by currency symbol.  If
              doubled, replaced by international currency symbol.
              If present in a pattern, the monetary decimal separator
              is used instead of the decimal separator.
     <tr valign=top>
          <td><code>'</code>
          <td>Prefix or suffix
          <td>Used to quote special characters in a prefix or suffix,
              for example, <code>"'#'#"</code> formats 123 to
              <code>"#123"</code>.  To create a single quote
              itself, use two in a row: <code>"# o''clock"</code>.
 </table>

### Format Specifiers for Date/Time Conversions

Use the following format specifiers for date/time conversions:

<table>
  <tr>
    <th>Symbol</th>
    <th>Meaning</th>
    <th>Presentation</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td>G</td>
    <td>era</td>
    <td>text</td>
    <td>AD</td>
  </tr>
  <tr>
    <td>C</td>
    <td>century of era (&gt;=0)</td>
    <td>number</td>
    <td>20</td>
  </tr>
  <tr>
    <td>Y</td>
    <td>year of era (&gt;=0)</td>
    <td>year</td>
    <td>1996</td>
  </tr>
  <tr>
    <td>x</td>
    <td>weekyear</td>
    <td>year</td>
    <td>1996</td>
  </tr>
  <tr>
    <td>w</td>
    <td>week of weekyear</td>
    <td>number</td>
    <td>27</td>
  </tr>
  <tr>
    <td>e</td>
    <td>day of week</td>
    <td>number</td>
    <td>2</td>
  </tr>
  <tr>
    <td>E</td>
    <td>day of week</td>
    <td>text</td>
    <td>Tuesday; Tue</td>
  </tr>
  <tr>
    <td>y</td>
    <td>year</td>
    <td>year</td>
    <td>1996</td>
  </tr>
  <tr>
    <td>D</td>
    <td>day of year</td>
    <td>number</td>
    <td>189</td>
  </tr>
  <tr>
    <td>M</td>
    <td>month of year</td>
    <td>month</td>
    <td>July; Jul; 07</td>
  </tr>
  <tr>
    <td>d</td>
    <td>day of month</td>
    <td>number</td>
    <td>10</td>
  </tr>
  <tr>
    <td>a</td>
    <td>halfday of day</td>
    <td>text</td>
    <td>PM</td>
  </tr>
  <tr>
    <td>K</td>
    <td>hour of halfday (0~11)</td>
    <td>number</td>
    <td>0</td>
  </tr>
  <tr>
    <td>h</td>
    <td>clockhour of halfday (1~12)number</td>
    <td>12</td>
    <td></td>
  </tr>
  <tr>
    <td>H</td>
    <td>hour of day (0~23)</td>
    <td>number</td>
    <td>0</td>
  </tr>
  <tr>
    <td>k</td>
    <td>clockhour of day (1~24)</td>
    <td>number</td>
    <td>24</td>
  </tr>
  <tr>
    <td>m</td>
    <td>minute of hour</td>
    <td>number</td>
    <td>30</td>
  </tr>
  <tr>
    <td>s</td>
    <td>second of minute</td>
    <td>number</td>
    <td>55</td>
  </tr>
  <tr>
    <td>S</td>
    <td>fraction of second</td>
    <td>number</td>
    <td>978</td>
  </tr>
  <tr>
    <td>z</td>
    <td>time zone</td>
    <td>text</td>
    <td>Pacific Standard Time; PST</td>
  </tr>
  <tr>
    <td>Z</td>
    <td>time zone offset/id</td>
    <td>zone</td>
    <td>-0800; -08:00; America/Los_Angeles</td>
  </tr>
  <tr>
    <td>escape for text delimiter   '</td>
    <td>single quote</td>
    <td>literal</td>
    <td></td>
  </tr>
</table>

For more information about specifying a format, refer to one of the following format specifier documents:

* [Java DecimalFormat class](http://docs.oracle.com/javase/7/docs/api/java/text/DecimalFormat.html) format specifiers 
* [Java DateTimeFormat class](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) format specifiers

## TO_CHAR

TO_CHAR converts a date, time, timestamp, or numerical expression to a character string.

### Syntax

    TO_CHAR (expression, 'format');

*expression* is a float, integer, decimal, date, time, or timestamp expression. 

*'format'* is format specifier enclosed in single quotation marks that sets a pattern for the output formatting. 

### Usage Notes


### Examples

Convert a float to a character string.

    SELECT TO_CHAR(125.789383, '#,###.###') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 125.789    |
    +------------+

Convert an integer to a character string.

    SELECT TO_CHAR(125, '#,###.###') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 125        |
    +------------+
    1 row selected (0.083 seconds)

Convert a date to a character string.

    SELECT TO_CHAR((CAST('2008-2-23' AS DATE)), 'yyyy-MMM-dd') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2008-Feb-23 |
    +------------+

Convert a time to a string.

    SELECT TO_CHAR(CAST('12:20:30' AS TIME), 'HH mm ss') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 12 20 30   |
    +------------+
    1 row selected (0.07 seconds)


Convert a timestamp to a string.

    SELECT TO_CHAR(CAST('2015-2-23 12:00:00' AS TIMESTAMP), 'yyyy MMM dd HH:mm:ss') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015 Feb 23 12:00:00 |
    +------------+
    1 row selected (0.075 seconds)

## TO_DATE
Converts a character string or a UNIX epoch timestamp to a date.

### Syntax

    TO_DATE (expression [, 'format']);

*expression* is a character string enclosed in single quotation marks or a Unix epoch timestamp in milliseconds, not enclosed in single quotation marks. 

* 'format'* is format specifier enclosed in single quotation marks that sets a pattern for the output formatting. Use this option only when the expression is a character string, not a UNIX epoch timestamp. 

### Usage 
Specify a format using patterns defined in [Java DateTimeFormat class](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html). The TO_TIMESTAMP function takes a Unix epoch timestamp. The TO_DATE function takes a UNIX epoch timestamp in milliseconds.


### Examples
The first example converts a character string to a date. The second example extracts the year to verify that Drill recognizes the date as a date type. 

    SELECT TO_DATE('2015-FEB-23', 'yyyy-MMM-dd') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-02-23 |
    +------------+
    1 row selected (0.077 seconds)

    SELECT EXTRACT(year from mydate) `extracted year` FROM (SELECT TO_DATE('2015-FEB-23', 'yyyy-MMM-dd') AS mydate FROM sys.drillbits);

    +------------+
    |   myyear   |
    +------------+
    | 2015       |
    +------------+
    1 row selected (0.128 seconds)

The following example converts a UNIX epoch timestamp to a date.

    SELECT TO_DATE(1427849046000) FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-04-01 |
    +------------+
    1 row selected (0.082 seconds)

## TO_NUMBER

TO_NUMBER converts a character string to a formatted number using a format specification.

### Syntax

    TO_NUMBER ('string', 'format');

*'string'* is a character string enclosed in single quotation marks. 

* 'format'* is one or more [Java DecimalFormat class](http://docs.oracle.com/javase/7/docs/api/java/text/DecimalFormat.html) format specifiers enclosed in single quotation marks that set a pattern for the output formatting.


### Usage Notes
The data type of the output of TO_NUMBER is a numeric. You can use the following [Java DecimalFormat class](http://docs.oracle.com/javase/7/docs/api/java/text/DecimalFormat.html) format specifiers to set the output formatting. 

* #  
  Digit place holder. 

* 0  
  Digit place holder. If a value has a digit in the position where the '0' appears in the format string, that digit appears in the output; otherwise, a '0' appears in that position in the output.

* .  
  Decimal point. Make the first '.' character in the format string the location of the decimal separator in the value; ignore any additional '.' characters.

* ,  
  Comma grouping separator. 

* E
  Exponent. Separates mantissa and exponent in scientific notation. 

### Examples

    SELECT TO_NUMBER('987,966', '######') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 987.0      |
    +------------+

    SELECT TO_NUMBER('987.966', '###.###') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 987.966    |
    +------------+
    1 row selected (0.063 seconds)

    SELECT TO_NUMBER('12345', '##0.##E0') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 12345.0    |
    +------------+
    1 row selected (0.069 seconds)

## TO_TIME
Converts a character string to a time.

### Syntax

    TO_TIME (expression [, 'format']);

*expression* is a character string enclosed in single quotation marks or milliseconds, not enclosed in single quotation marks. 

* 'format'* is format specifier enclosed in single quotation marks that sets a pattern for the output formatting. Use this option only when the expression is a character string, not milliseconds. 

## Usage 
Specify a format using patterns defined in [Java DateTimeFormat class](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html).

### Examples

    SELECT TO_TIME('12:20:30', 'HH:mm:ss') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 12:20:30   |
    +------------+
    1 row selected (0.067 seconds)

Convert 828550000 milliseconds (23 hours 55 seconds) to the time.

    SELECT to_time(82855000) FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 23:00:55   |
    +------------+
    1 row selected (0.086 seconds)

## TO_TIMESTAMP

### Syntax

    TO_TIMESTAMP (expression [, 'format']);

*expression* is a character string enclosed in single quotation marks or a UNIX epoch timestamp, not enclosed in single quotation marks. 

* 'format'* is format specifier enclosed in single quotation marks that sets a pattern for the output formatting. Use this option only when the expression is a character string, not a UNIX epoch timestamp. 

### Usage 
Specify a format using patterns defined in [Java DateTimeFormat class](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html). The TO_TIMESTAMP function takes a Unix epoch timestamp. The TO_DATE function takes a UNIX epoch timestamp in milliseconds.

### Examples

Convert a date to a timestamp. 

    SELECT TO_TIMESTAMP('2008-2-23 12:00:00', 'yyyy-MM-dd HH:mm:ss') FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2008-02-23 12:00:00.0 |
    +------------+

Convert a Unix Epoch time to a timestamp.

    SELECT TO_TIMESTAMP(1427936330) FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-04-01 17:58:50.0 |
    +------------+
    1 row selected (0.094 seconds)

Connvert a UTC date to a timestamp offset from the UTC time zone code.

    SELECT TO_TIMESTAMP('2015-03-30 20:49:59.0 UTC', 'YYYY-MM-dd HH:mm:ss.s z') as Original, TO_CHAR(TO_TIMESTAMP('2015-03-30 20:49:59.0 UTC', 'YYYY-MM-dd HH:mm:ss.s z'), 'z') AS New_TZ FROM sys.drillbits;

    +------------+------------+
    |  Original  |   New_TZ   |
    +------------+------------+
    | 2015-03-30 20:49:00.0 | UTC        |
    +------------+------------+
    1 row selected (0.129 seconds)


<!-- DRILL-448 Support timestamp with time zone -->


<!-- Apache Drill    
Apache DrillDRILL-1141
ISNUMERIC should be implemented as a SQL function
SELECT count(columns[0]) as number FROM dfs.`bla` WHERE ISNUMERIC(columns[0])=1
 -->