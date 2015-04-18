---
title: "Supported Data Types"
parent: "Data Types"
---
Drill supports the following SQL data types:

* BIGINT  
  8-byte signed integer in the range -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.

* BINARY
  Variable-length byte string

* BOOLEAN  
  True or false  

* DATE  
  Years, months, and days in YYYY-MM-DD format since 4713 BC.

* DECIMAL(p,s), or DEC(p,s), NUMERIC(p,s)  
  38-digit precision number, precision is p, and scale is s. Example: DECIMAL(6,2) has 4 digits before the decimal point and 2 digits after the decimal point. 

* FLOAT  
  4-byte floating point number

* DOUBLE, DOUBLE PRECISION**  
  8-byte floating point number, precision-scalable 

* INTEGER or INT  
  4-byte signed integer in the range -2,147,483,648 to 2,147,483,647

* INTERVALDAY  
  A simple version of the interval type expressing a period of time in days, hours, minutes, and seconds only

* INTERVALYEAR  
  A simple version of interval representing a period of time in years and months only

* SMALLINT*  
  2-byte signed integer in the range -32,768 to 32,767

* TIME  
  24-hour based time before or after January 1, 2001 in hours, minutes, seconds format: HH:mm:ss 

* TIMESTAMP  
  JDBC timestamp in year, month, date hour, minute, second, and optional milliseconds format: yyyy-MM-dd HH:mm:ss.SSS

* CHARACTER VARYING, CHARACTER, CHAR, or VARCHAR  
  UTF8-encoded variable-length string. For example, CHAR(30) casts data to a 30-character string maximum. The default limit is 1 character. The maximum character limit is 255.

\* Not currently supported.  
\*\* You specify a DECIMAL using a precision and scale. The precision (p) is the total number of digits required to represent the number. The scale (s) is the number of decimal digits to the right of the decimal point. Subtract s from p to determine the maximum number of digits to the left of the decimal point. Scale is a value from 0 through p. Scale is specified only if precision is specified. The default scale is 0.  

## Using Drill Data Types

In Drill, you use data types in the following ways:

* To cast or convert data to the required type for moving data from one data source to another
* To cast or convert data to the required type for Drill analysis

In Drill, assign a data type to every column name in a CREATE TABLE statement as you do in database software. Instead, you use the CREATE TABLE AS SELECT (CTAS) statement with one or more of the following functions to define the type of a column:

* [CAST]({{ site.baseurl }}/docs/data-type-conversion#cast)  
  Use the supported SQL data types listed at the beginning of this page.  
* [CONVERT TO/FROM]({{ site.baseurl }}/docs/data-type-conversion#convert_to-and-convert_from)
  Use the [CONVERT TO AND CONVERT FROM data types]({{ site.baseurl }}/docs/supported-data-types/#convert_to-and-convert_from-data-types)  
* Other [data conversion functions]({{ site.baseurl }}/docs/data-type-conversion#other-data-type-conversions)
  Use the syntax described in the function descriptions. 

Keep the following best practices for converting to/from binary data:

* Use CAST for converting INT and BIGINT to/from binary types.
* Use CONVERT_TO and CONVERT_FROM for converting other types to/from binary. 

In some cases, Drill converts schema-less data to correctly-typed data implicitly. In this case, you do not need to cast the data. The file format of the data and the nature of your query determines the requirement for casting or converting. Differences in casting depend on the data source. The following list describes how Drill treats data types from various data sources:

* HBase  
  Does not implicitly cast input to SQL types. Convert data to appropriate types as shown in ["Querying HBase."]({{ site.baseurl }}/docs/querying-hbase/)
* Hive  
  Implicitly casts Hive types to SQL types as shown in the Hive [type mapping example]({{ site.baseurl }}/docs/hive-to-drill-data-type-mapping#type-mapping-example)
* JSON  
  Implicitly casts JSON data to its [corresponding SQL types]({{ site.baseurl }}/docs/json-data-model#data-type-mapping) or to VARCHAR if Drill is in all text mode. 
* MapR-DB  
  Implicitly casts MapR-DB data to SQL types when you use [the maprdb format]({{ site.baseurl }}/docs/mapr-db-format) for reading MapR-DB data. The dfs storage plugin defines the format when you install Drill from the mapr-drill package on a MapR node.
* Parquet  
  Implicitly casts Parquet data to the SQL types shown in [SQL Data Types to Parquet]({{ site.baseurl }}/docs/parquet-format#sql-data-types-to-parquet). 
* Text: CSV, TSV, and other text  
  Implicitly casts all textual data to VARCHAR.

## Precedence of Data Types

Drill reads from and writes to data sources having a wide variety of types, more types than those Drill [previously listed]({{ site.baseurl }}/docs/supported-data-types). Drill uses data types at the RPC level that are not supported for query input, often implicitly casting data.

The following list includes data types Drill uses in descending order of precedence. As shown in the table, you can cast a NULL value, which has the lowest precedence, to any other type; you can cast a SMALLINT value to INT. You cannot cast an INT value to SMALLINT due to possible precision loss. Drill might deviate from these precedence rules for performance reasons. Under certain circumstances, such as queries involving SUBSTR and CONCAT functions, Drill reverses the order of precedence and allows a cast to VARCHAR from a type of higher precedence than VARCHAR, such as BIGINT.

### Casting Precedence

<table>
  <tr>
    <th>Precedence</th>
    <th>Data Type</th>
    <th>Precedence</th>
    <th>Data Type</th>
  </tr>
  <tr>
    <td>1</td>
    <td>INTERVALYEAR (highest)</td>
    <td>11</td>
    <td>INT</td>
  </tr>
  <tr>
    <td>2</td>
    <td>INTERVLADAY</td>
    <td>12</td>
    <td>UINT2</td>
  </tr>
  <tr>
    <td>3</td>
    <td>TIMESTAMP</td>
    <td>13</td>
    <td>SMALLINT</td>
  </tr>
  <tr>
    <td>4</td>
    <td>DATE</td>
    <td>14</td>
    <td>UINT1</td>
  </tr>
  <tr>
    <td>5</td>
    <td>TIME</td>
    <td>15</td>
    <td>VAR16CHAR</td>
  </tr>
  <tr>
    <td>6</td>
    <td>DOUBLE</td>
    <td>16</td>
    <td>FIXED16CHAR</td>
  </tr>
  <tr>
    <td>7</td>
    <td>DECIMAL</td>
    <td>17</td>
    <td>VARCHAR</td>
  </tr>
  <tr>
    <td>8</td>
    <td>UINT8</td>
    <td>18</td>
    <td>CHAR</td>
  </tr>
  <tr>
    <td>9</td>
    <td>BIGINT</td>
    <td>19</td>
    <td>VARBINARY*</td>
  </tr>
  <tr>
    <td>10</td>
    <td>UINT4</td>
    <td>20</td>
    <td>FIXEDBINARY**</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>21</td>
    <td>NULL (lowest)</td>
  </tr>
</table>

\* The Drill Parquet reader supports these types.

## Explicit Casting

In a textual file, such as CSV, Drill interprets every field as a VARCHAR, as previously mentioned. To handle textual data, you can use the following functions to cast and convert compatible data types:

* [CAST]({{ site.baseurl }}/docs/data-type-conversion#cast)  
  Casts data from one data type to another.
* [CONVERT_TO and CONVERT_FROM]({{ site.baseurl }}/docs/data-type-conversion#convert_to-and-convert_from)  
  Converts data, including binary data, from one data type to another.
* [TO_CHAR]()  
  Converts a TIMESTAMP, INTERVALDAY/INTERVALYEAR, INTEGER, DOUBLE, or DECIMAL to a string.
* [TO_DATE]()  
  Converts a string to DATE.
* [TO_NUMBER]()  
  Converts a string to a DECIMAL.
* [TO_TIMESTAMP]()  
  Converts a string to TIMESTAMP.

If the SELECT statement includes a WHERE clause that compares a column of an unknown data type, cast both the value of the column and the comparison value in the WHERE clause.

## Explicit Type Casting Maps

The following tables show data types that Drill can cast to/from other data types. Not all types are available for explicit casting in the current release.

### Numerical and Character Data Types

<table>
  <tr>
    <th></th>
    <th>To:</th>
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
    <td>BIGINT</td>
    <td>DECIMAL</td>
    <td>FLOAT</td>
    <td>CHAR</td>
    <td>FIXEDBINARY</td>
    <td>VARCHAR</td>
    <td>VARBINARY</td>
  </tr>
  <tr>
    <td>SMALLINT*</td>
    <td></td>
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
  </tr>
  <tr>
    <td>BIGINT</td>
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
  </tr>
  <tr>
    <td>DOUBLE</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>yes</td>
    <td>no</td>
    <td>yes</td>
    <td>no</td>
  </tr>
  <tr>
    <td>FLOAT</td>
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
    <td>CHAR</td>
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
    <td>FIXEDBINARY**</td>
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
    <td>VARCHAR***</td>
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
    <td>VARBINARY**</td>
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
\* Not supported in this release.   
\*\* Used to cast binary data other than INT and BIGINT coming to/from sources such as MapR-DB/HBase.   
\*\*\* You cannot convert a character string having a decimal point to an INT or BIGINT.   

### Date and Time Data Types

<table>
  <tr>
    <th></th>
    <th>To:</th>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
    <th></th>
  </tr>
  <tr>
    <td>From:</td>
    <td>DATE</td>
    <td>TIME</td>
    <td>TIMESTAMP</td>
    <td>INTERVALYEAR</td>
    <td>INTERVALDAY</td>
  </tr>
  <tr>
    <td>CHAR</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
  </tr>
  <tr>
    <td>FIXEDBINARY*</td>
    <td>No</td>
    <td>No</td>
    <td>No</td>
    <td>Yes</td>
    <td>No</td>
    <td>No</td>
    <td>No</td>
  </tr>
  <tr>
    <td>VARCHAR</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
  </tr>
  <tr>
    <td>VARBINARY*</td>
    <td>No</td>
    <td>No</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>No</td>
    <td>No</td>
    <td>No</td>
  </tr>
  <tr>
    <td>DATE</td>
    <td>No</td>
    <td>No</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>No</td>
    <td>No</td>
    <td>No</td>
  </tr>
  <tr>
    <td>TIME</td>
    <td>No</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>No</td>
    <td>No</td>
    <td>No</td>
  </tr>
  <tr>
    <td>TIMESTAMP</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>No</td>
    <td>No</td>
    <td>No</td>
  </tr>
  <tr>
    <td>INTERVALYEAR</td>
    <td>Yes</td>
    <td>No</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>No</td>
    <td>Yes</td>
  </tr>
  <tr>
    <td>INTERVALDAY</td>
    <td>Yes</td>
    <td>No</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>No</td>
  </tr>
</table>
\* Used to cast binary data coming to/from sources such as MapR-DB/HBase.   

## CONVERT_TO and CONVERT_FROM Data Types

The following table lists the data types for use with the CONVERT_TO
and CONVERT_FROM functions:

**Type**| **Input Type**| **Output Type**  
---|---|---  
JSON | bytes | varchar
BOOLEAN_BYTE| bytes(1)| boolean  
TINYINT_BE| bytes(1)| tinyint  
TINYINT| bytes(1)| tinyint  
SMALLINT_BE| bytes(2)| smallint  
SMALLINT| bytes(2)| smallint  
INT_BE| bytes(4)| int  
INT| bytes(4)| int  
BIGINT_BE| bytes(8)| bigint  
BIGINT| bytes(8)| bigint  
FLOAT| bytes(4)| float (float4)  
DOUBLE| bytes(8)| double (float8)  
INT_HADOOPV| bytes(1-9)| int  
BIGINT_HADOOPV| bytes(1-9)| bigint  
DATE_EPOCH_BE| bytes(8)| date  
DATE_EPOCH| bytes(8)| date  
TIME_EPOCH_BE| bytes(8)| time  
TIME_EPOCH| bytes(8)| time  
UTF8| bytes| varchar  
UTF16| bytes| var16char  
UINT8| bytes(8)| uint8  

If you are unsure that the size of the source and destination INT or BIGINT you are converting is the same, use CAST to convert these data types to/from binary.


