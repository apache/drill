---
title: "Data Types"
parent: "SQL Reference"
---
Depending on the data format, you might need to cast or convert data types when Drill reads/writes data.

After Drill reads schema-less data into SQL tables, you need to cast data types explicitly to query the data. In some cases, Drill converts schema-less data to typed data implicitly. In this case, you do not need to cast. The file format of the data and the nature of your query determines the requirement for casting or converting. 

Differences in casting depend on the data source. The following list describes how Drill treats data types from various data sources:

* HBase  
  Does not implicitly cast input to SQL types. Convert data to appropriate types as shown in ["Querying HBase."](/docs/querying-hbase/)
* Hive  
  Implicitly casts Hive types to SQL types as shown in the Hive [type mapping example](/docs/hive-to-drill-data-type-mapping#type-mapping-example)
* JSON  
  Implicitly casts JSON data to its [corresponding SQL types](/docs/json-data-model#data-type-mapping) or to VARCHAR if Drill is in all text mode. 
* MapR-DB  
  Implicitly casts MapR-DB data to SQL types when you use [the maprdb format](/docs/mapr-db-format) for reading MapR-DB data. The dfs storage plugin defines the format when you install Drill from the mapr-drill package on a MapR node.
* Parquet  
  Implicitly casts Parquet data to the SQL types shown in [SQL Data Types to Parquet](/docs/parquet-format/sql-data-types-to-parquet). 
* Text: CSV, TSV, and other text  
  Implicitly casts all textual data to VARCHAR.

## Implicit Casting


Generally, Drill performs implicit casting based on the order of precedence shown in the implicit casting preference table. Drill usually implicitly casts a type from a lower precedence to a type having higher precedence. For instance, NULL can be promoted to any other type; SMALLINT can be promoted into INT. INT is not promoted to SMALLINT due to possible precision loss. Drill might deviate from these precedence rules for performance reasons.

Under certain circumstances, such as queries involving substr and concat functions, Drill reverses the order of precedence and allows a cast to VARCHAR from a type of higher precedence than VARCHAR, such as BIGINT. 

The following table lists data types top to bottom, in descending order of precedence. Drill implicitly casts to more data types than are currently supported for explicit casting.

### Implicit Casting Precedence

<table>
  <tr>
    <th>Precedence</th>
    <th>Data Type</th>
    <th>Precedence</th>
    <th>Data Type</th>
  </tr>
  <tr>
    <td>1</td>
    <td>INTERVAL</td>
    <td>13</td>
    <td>UINT4</td>
  </tr>
  <tr>
    <td>2</td>
    <td>INTERVALYEAR</td>
    <td>14</td>
    <td>INT</td>
  </tr>
  <tr>
    <td>3</td>
    <td>INTERVLADAY</td>
    <td>15</td>
    <td>UINT2</td>
  </tr>
  <tr>
    <td>4</td>
    <td>TIMESTAMPTZ</td>
    <td>16</td>
    <td>SMALLINT</td>
  </tr>
  <tr>
    <td>5</td>
    <td>TIMETZ</td>
    <td>17</td>
    <td>UINT1</td>
  </tr>
  <tr>
    <td>6</td>
    <td>TIMESTAMP</td>
    <td>18</td>
    <td>VAR16CHAR</td>
  </tr>
  <tr>
    <td>7</td>
    <td>DATE</td>
    <td>19</td>
    <td>FIXED16CHAR</td>
  </tr>
  <tr>
    <td>8</td>
    <td>TIME</td>
    <td>20</td>
    <td>VARCHAR</td>
  </tr>
  <tr>
    <td>9</td>
    <td>DOUBLE</td>
    <td>21</td>
    <td>CHAR</td>
  </tr>
  <tr>
    <td>10</td>
    <td>DECIMAL</td>
    <td>22</td>
    <td>VARBINARY*</td>
  </tr>
  <tr>
    <td>11</td>
    <td>UINT8</td>
    <td>23</td>
    <td>FIXEDBINARY*</td>
  </tr>
  <tr>
    <td>12</td>
    <td>BIGINT</td>
    <td>24</td>
    <td>NULL</td>
  </tr>
</table>

\* The Drill Parquet reader supports these types.

## Explicit Casting

In a textual file, such as CSV, Drill interprets every field as a VARCHAR, as previously mentioned. To handle textual data, you can use the following functions to cast and convert compatible data types:

* [CAST](/docs/data-type-conversion#cast)  
  Casts data from one data type to another.
* [CONVERT_TO and CONVERT_FROM](/docs/conversion#convert-to-and-convert-from)  
  Converts data, including binary data, from one data type to another.
* [TO_CHAR]()  
  Converts a TIMESTAMP, INTERVAL, INTEGER, DOUBLE, or DECIMAL to a string.
* [TO_DATE]()  
  Converts a string to DATE.
* [TO_NUMBER]()  
  Converts a string to a DECIMAL.
* [TO_TIMESTAMP]()  
  Converts a string to TIMESTAMP.

If the SELECT statement includes a WHERE clause that compares a column of an unknown data type, cast both the value of the column and the comparison value in the WHERE clause.

## Supported Data Types for Casting
You use the following data types in queries that involve casting/converting data types:

* BIGINT  
  8-byte signed integer. the range is -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.

* BOOLEAN  
  True or false  

* DATE  
  Years, months, and days in YYYY-MM-DD format

* DECIMAL(p,s), or DEC(p,s), NUMERIC(p,s) 
  38-digit precision number, precision is p, and scale is s. Example: DECIMAL(6,2) has 4 digits before the decimal point and 2 digits after the decimal point. 

* FLOAT  
  4-byte single precision floating point number

* DOUBLE, DOUBLE PRECISION  
  8-byte double precision floating point number. 

* INTEGER or INT  
  4-byte signed integer. The range is -2,147,483,648 to 2,147,483,647.

* INTERVAL  
  Integer fields representing a period of time in years, months, days hours, minutes, seconds and optional milliseconds using ISO 8601 format.

* INTERVALDAY  
  A simple version of the interval type expressing a period of time in days, hours, minutes, and seconds only.

* INTERVALYEAR  
  A simple version of interval representing a period of time in years and months only.

* SMALLINT  
  2-byte signed integer. The range is -32,768 to 32,767. Supported in Drill 0.9 and later. See DRILL-2135.

* TIME  
  Hours, minutes, seconds in the form HH:mm:ss, 24-hour based

* TIMESTAMP  
  JDBC timestamp in year, month, date hour, minute, second, and optional milliseconds: yyyy-MM-dd HH:mm:ss.SSS

* CHARACTER VARYING, CHARACTER, CHAR, or VARCHAR  
  Character string optionally declared with a length that indicates the maximum number of characters to use. For example, CHAR(30) casts data to a 30-character string maximum. The default limit is 1 character. The maximum character limit is 255.

You specify a DECIMAL using a precision and scale. The precision (p) is the total number of digits required to represent the number.
. The scale (s) is the number of decimal digits to the right of the decimal point. Subtract s from p to determine the maximum number of digits to the left of the decimal point. Scale is a value from 0 through p. Scale is specified only if precision is specified. The default scale is 0.

For more information about and examples of casting, see [CAST]().

### Explicit Type Casting Maps

The following tables show data types that Drill can cast to/from other data types. Not all types are available for explicit casting in the current release.

#### Numerical and Character Data Types

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
    <td>SMALLINT</td>
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
    <td>FIXEDBINARY*</td>
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
    <td>VARCHAR**</td>
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
    <td>VARBINARY*</td>
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

\* For use with CONVERT_TO/FROM to cast binary data coming to/from sources such as MapR-DB/HBase.

\*\* You cannot convert a character string having a decimal point to an INT or BIGINT.

#### Date and Time Data Types

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
    <td>TIMESTAMPTZ</td>
    <td>INTERVAL</td>
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
    <td>FIXEDBINARY</td>
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
    <td>VARBINARY</td>
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
    <td>TIMESTAMPTZ</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>No</td>
    <td>No</td>
    <td>No</td>
    <td>No</td>
  </tr>
  <tr>
    <td>INTERVAL</td>
    <td>Yes</td>
    <td>No</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>No</td>
    <td>Yes</td>
    <td>Yes</td>
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

