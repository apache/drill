---
title: "Handling Disparate Data Types"
parent: "Data Type Casting"
---
[Previous](/docs/supported-date-time-data-type-formats)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Back to Table of Contents](/docs)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Next](/docs/lexical-structure)

The file format of the data and queries you plan to use determine the casting or converting you need to do, if any. When Drill reads schema-less data into SQL tables for querying, you might need to cast one data type to another explicitly. In some cases, Drill converts schema-less data to typed data implicitly. In this case, you do not need to cast. Drill does not implicitly cast HBase binary data. You use CONVERT_TO and CONVERT_FROM functions to work with HBase data in Drill.

The following list describes how Drill treats data types from various data sources:

* HBase  
  No implicit casting to SQL types. Convert data to appropriate types as shown in ["Querying HBase."](/docs/querying-hbase/)
* Hive  
  Implicitly casts Hive types to SQL types as shown in the Hive [type mapping example](/docs/hive-to-drill-data-type-mapping#type-mapping-example)
* JSON  
  Implicitly casts JSON data to its [corresponding SQL types](/docs/json-data-model#data-type-mapping) or to VARCHAR if Drillis in all text mode. 
* MapR-DB  
  Implicitly casts MapR-DB data to SQL types when you use [the maprdb format](/docs/mapr-db-format) for reading MapR-DB data. The dfs storage plugin defines the format when you install Drill from the mapr-drill package on a MapR node.
* Parquet  
  Implicitly casts Parquet data to the SQL types shown in [SQL Data Types to Parquet](/docs/parquet-format/sql-data-types-to-parquet). 
* Text: CSV, TSV, and other text  
  Implicitly casts all textual data to VARCHAR.

## Implicit Casting


In general, Drill implicitly casts (promotes) one type to another type based in the order of precedence, high to low, shown in the following table. A type that has a lower precedence can be implicitly cast to type of higher precedence. For instance, NULL can be promoted to any other type; SMALLINT can be promoted into INT. INT cannot be promoted to SMALLINT due to possible precision loss.

Under certain circumstances, such as queries involving functions such as substr and concat, Drill reverses the order of precedence and allows a cast to VARCHAR from a type of higher precedence, such as BIGINT. Drill implicitly casts to more data types than currently supported for explicit casting.
 
<table>
  <tr>
    <th>Data Types by Precedence (high to low)</th>
    <th></th>
  </tr>
  <tr>
    <td>24 INTERVAL</td>
    <td>12 BIGINT</td>
  </tr>
  <tr>
    <td>23 INTERVALYEAR</td>
    <td>11UINT4</td>
  </tr>
  <tr>
    <td>22 INTERVLADAY</td>
    <td>10 INT</td>
  </tr>
  <tr>
    <td>21 TIMESTAMPTZ</td>
    <td>9 UINT2</td>
  </tr>
  <tr>
    <td>20 TIMETZ</td>
    <td>8 SMALLINT</td>
  </tr>
  <tr>
    <td>19 TIMESTAMP</td>
    <td>7 UINT1</td>
  </tr>
  <tr>
    <td>18 DATE</td>
    <td>6 VAR16CHAR</td>
  </tr>
  <tr>
    <td>17 TIME</td>
    <td>5 FIXED16CHAR</td>
  </tr>
  <tr>
    <td>16 FLOAT8</td>
    <td>4 VARCHAR</td>
  </tr>
  <tr>
    <td>15 DECIMAL</td>
    <td>3 FIXEDCHAR</td>
  </tr>
  <tr>
    <td>14 MONEY</td>
    <td>2 VARBINARY</td>
  </tr>
  <tr>
    <td>13 UINT8</td>
    <td>1 FIXEDBINARY</td>
  </tr>
  <tr>
    <td></td>
    <td>0 NULL</td>
  </tr>
</table>

## Explicit Casting

Drill supports a number of functions to cast and convert compatible data types:

* CAST  
  Casts textual data from one data type to another.
* CONVERT_TO and CONVERT_FROM  
  Converts data, including binary data, from one data type to another.
* TO_CHAR
  Converts a TIMESTAMP, INTERVAL, INTEGER, DOUBLE, or DECIMAL to a string.
* TO_DATE
  Converts a string to DATE.
* TO_NUMBER
  Converts a string to a DECIMAL.
* TO_TIMESTAMP
  Converts a string to TIMESTAMP.


### Using CAST

Embed a CAST function in a query using this syntax:

    cast <expression> AS <data type> 

* expression  
  An entity that has single data value, such as a column name, of the data type you want to cast to a different type
* data type  
  The target data type, such as INTEGER or DATE

Example: Inspect INTEGER data and cast the data to the DECIMAL type

    SELECT c_row, c_int FROM mydata WHERE c_row = 9;

    c_row | c_int
    ------+------------
        9 | -2147483648
    (1 row)

    SELECT c_row, CAST(c_int AS DECIMAL(28,8)) FROM my_data WHERE c_row = 9;

    c_row | c_int
    ------+---------------------
    9     | -2147483648.00000000
    (1 row)

If the SELECT statement includes a WHERE clause that compares a column of an unknown data type, cast both the value of the column and the comparison value in the WHERE clause. For example:

    SELECT c_row, CAST(c_int AS DECIMAL(28,8)) FROM mydata WHERE CAST(c_int AS CECIMAL(28,8)) > -3.0

Although you can use CAST to handle binary data, CONVERT_TO and CONVERT_FROM are recommended for these conversions.

The following table shows data types that you can cast to from other data types.

<table>
  <tr>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
  </tr>
  <tr>
    <td>From:</td>
    <td>SMALLINT</td>
    <td>INT</td>
    <td>BIGINT/UINT</td>
    <td>DECIMAL</td>
    <td>FLOAT4</td>
    <td>FLOAT8</td>
    <td>FIXEDCHAR</td>
    <td>FIXEDBINARY</td>
    <td>VARCHAR</td>
    <td>VARBINARY</td>
  </tr>

  <tr>
    <td>SMALLINT</td>
    <td>no</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
  </tr>
  <tr>
    <td>INT</td>
    <td>yes</td>
    <td>no</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
  </tr>
  <tr>
    <td>BIGINT/UINT</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
  </tr>
  <tr>

  <tr>
    <td>DECIMAL</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
  </tr>

  <tr>
    <td>FLOAT8</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>no</td>
    <td>yes</td>
    <td>no</td>
    <td>yes</td>
    <td>no</td>
  </tr>
  <tr>
    <td>FLOAT4</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>no</td>
    <td>no</td>
    <td>yes</td>
    <td>no</td>
    <td>yes</td>
    <td>no</td>
  </tr>
  
  <tr>
    <td>FIXEDCHAR</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>no</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
  </tr>
  <tr>
    <td>FIXEDBINARY</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>no</td>
    <td>no</td>
    <td>yes</td>
    <td>yes</td>
  </tr>
  <tr>
    <td>VARCHAR</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>no</td>
    <td>yes</td>
  </tr>
  <tr>
    <td>VARBINARY</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>no</td>
    <td>yes</td>
    <td>yes</td>
    <td>no</td>
  </tr>

</table>

### Using CONVERT_TO and CONVERT_FROM

To query HBase data in Drill, convert every column of an HBase table to/from byte arrays from/to an [SQL data type](/docs/data-types/) that Drill supports when writing/reading data. For examples of how to use these functions, see ["Convert and Cast Functions".](/docs/sql-functions#convert-and-cast-functions)

## Handling Textual Data
In a textual file, such as CSV, Drill interprets every field as a VARCHAR, as previously mentioned. In addition to using the CAST function, you can also use [to_char](link), [to_date](line), [to_number](link), and [to_timestamp](link). If the SELECT statement includes a WHERE clause that compares a column of an unknown data type, cast both the value of the column and the comparison value in the WHERE clause.

## All text mode option
All text mode is a system option for controlling how Drill implicitly casts JSON data. When reading numerical values from a JSON file, Drill implicitly casts a number to the DOUBLE or BIGINT type depending on the presence or absence a decimal point. If some numbers in a JSON map or array appear with and without a decimal point, such as 0 and 0.0, Drill throws a schema change error. To prevent Drill from attempting to read such data, [set all_text_mode](/docs/json-data-model#handling-type-differences) to true. In all text mode, Drill implicitly casts JSON data to VARCHAR, which you can subsequently cast to desired types.

Drill reads numbers without decimal point as BIGINT values by default. The range of BIGINT is -9223372036854775808 to 9223372036854775807. A BIGINT result outside this range produces an error. Use `all_text_mode` to select data as VARCHAR and then cast the data to a numerical type.



