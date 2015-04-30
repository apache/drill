---
title: "Supported Data Types"
parent: "Data Types"
---
Drill reads from and writes to data sources having a wide variety of types. Drill uses data types at the RPC level that are not supported for query input, often implicitly casting data. Drill supports the following SQL data types for query input:

<table>
  <tr>
    <th>SQL Data Type</th>
    <th>Description</th>
    <th>Example</th>
  </tr>
  <tr>
    <td valign="top">BIGINT</td>
    <td valign="top">8-byte signed integer in the range -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807</td>
    <td valign="top">9223372036854775807</td>
  </tr>
  <tr>
    <td valign="top">BINARY VARYING, VARBINARY, BINARY</td>
    <td valign="top">Variable-length byte string</td>
    <td valign="top">B@e6d9eb7</td>
  </tr>
  <tr>
    <td valign="top">BOOLEAN</td>
    <td valign="top">True or false</td>
    <td valign="top">true</td>
  </tr>
  <tr>
    <td valign="top">DATE</td>
    <td valign="top">Years, months, and days in YYYY-MM-DD format since 4713 BC</td>
    <td valign="top">2015-12-30</td>
  </tr>
  <tr>
    <td valign="top">DECIMAL(p,s), or DEC(p,s), NUMERIC(p,s)***</td>
    <td valign="top">38-digit precision number, precision is p, and scale is s</td>
    <td valign="top">DECIMAL(6,2) is 1234.56,  4 digits before and 2 digits after the decimal point</td>
  </tr>
  <tr>
    <td valign="top">FLOAT</td>
    <td valign="top">4-byte floating point number</td>
    <td valign="top">0.456<br><a href="http://tshiran.github.io/drill/docs/handling-different-data-types/">See guidelines</a></td>
  </tr>
  <tr>
    <td valign="top">DOUBLE, DOUBLE PRECISION</td>
    <td valign="top">8-byte floating point number, precision-scalable. Precision (p) is digits in the number. Scale (s) is digits to the right of the decimal point. Specify both p and s or neither. Default is 1.</td>
    <td valign="top">0.456<br><a href="http://tshiran.github.io/drill/docs/handling-different-data-types/">See guidelines</a></td>
  </tr>
  <tr>
    <td valign="top">INTEGER or INT</td>
    <td valign="top">4-byte signed integer in the range -2,147,483,648 to 2,147,483,647</td>
    <td valign="top">2147483646</td>
  </tr>
  <tr>
    <td valign="top">INTERVALDAY</td>
    <td valign="top">A period of time in days, hours, minutes, and seconds only</td>
    <td valign="top">'1 10:20:30.123'<br><a href="http://tshiran.github.io/drill/docs/date-time-and-timestamp/#intervalyear-and-intervalday">More examples</a></td>
  </tr>
  <tr>
    <td valign="top">INTERVALYEAR</td>
    <td valign="top">A period of time in years and months only</td>
    <td valign="top">'1-2' year to month<br><a href="http://tshiran.github.io/drill/docs/data-type-conversion/#casting-intervals">More examples</a></td>
  </tr>
  <tr>
    <td valign="top">SMALLINT*</td>
    <td valign="top">2-byte signed integer in the range -32,768 to 32,767</td>
    <td valign="top">32000</td>
  </tr>
  <tr>
    <td valign="top">TIME</td>
    <td valign="top">24-hour based time before or after January 1, 2001 in hours, minutes, seconds format: HH:mm:ss</td>
    <td valign="top">22:55:55.23<br><a href="http://tshiran.github.io/drill/docs/date-time-and-timestamp/">More examples</a></td>
  </tr>
  <tr>
    <td valign="top">TIMESTAMP</td>
    <td valign="top">JDBC timestamp in year, month, date hour, minute, second, and optional milliseconds format: yyyy-MM-dd HH:mm:ss.SSS</td>
    <td valign="top">2015-12-30 22:55:55.23<br> <a href="http://tshiran.github.io/drill/docs/date-time-and-timestamp/">More examples</a></td>
  </tr>
  <tr>
    <td valign="top">CHARACTER VARYING, CHARACTER, CHAR, or VARCHAR**</td>
    <td valign="top">UTF8-encoded variable-length string. In this release, CHAR, its aliases, and VARCHAR types are not fundamentally different types. The default limit is 1 character. The maximum character limit is 2,147,483,647.</td>
    <td valign="top">CHAR(30) casts data to a 30-character string maximum.</td>
  </tr>
</table>
\* Not currently supported.  
\*\* Currently, Drill supports only variable-length strings. .
\*\*\* In this release the NUMERIC data type is an alias for the DECIMAL data type.
+  

## Casting and Converting Data Types

In Drill, you cast or convert data to the required type for moving data from one data source to another or to make the data readable.
You do not assign a data type to every column name in a CREATE TABLE statement to define the table as you do in database software. Instead, you use the CREATE TABLE AS SELECT (CTAS) statement with one or more of the following functions to define the table:

* [CAST]({{ site.baseurl }}/docs/data-type-conversion#cast)    
* [CONVERT TO/FROM]({{ site.baseurl }}/docs/data-type-conversion#convert_to-and-convert_from)   
  Use the [CONVERT TO AND CONVERT FROM data types]({{ site.baseurl }}/docs/supported-data-types/#convert_to-and-convert_from-data-types)  
* Other [data conversion functions]({{ site.baseurl }}/docs/data-type-conversion#other-data-type-conversions)   

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
    <td valign="top">1</td>
    <td valign="top">INTERVALYEAR (highest)</td>
    <td valign="top">11</td>
    <td valign="top">INT</td>
  </tr>
  <tr>
    <td valign="top">2</td>
    <td valign="top">INTERVLADAY</td>
    <td valign="top">12</td>
    <td valign="top">UINT2</td>
  </tr>
  <tr>
    <td valign="top">3</td>
    <td valign="top">TIMESTAMP</td>
    <td valign="top">13</td>
    <td valign="top">SMALLINT</td>
  </tr>
  <tr>
    <td valign="top">4</td>
    <td valign="top">DATE</td>
    <td valign="top">14</td>
    <td valign="top">UINT1</td>
  </tr>
  <tr>
    <td valign="top">5</td>
    <td valign="top">TIME</td>
    <td valign="top">15</td>
    <td valign="top">VAR16CHAR</td>
  </tr>
  <tr>
    <td valign="top">6</td>
    <td valign="top">DOUBLE</td>
    <td valign="top">16</td>
    <td valign="top">FIXED16CHAR</td>
  </tr>
  <tr>
    <td valign="top">7</td>
    <td valign="top">DECIMAL</td>
    <td valign="top">17</td>
    <td valign="top">VARCHAR</td>
  </tr>
  <tr>
    <td valign="top">8</td>
    <td valign="top">UINT8</td>
    <td valign="top">18</td>
    <td valign="top">CHAR</td>
  </tr>
  <tr>
    <td valign="top">9</td>
    <td valign="top">BIGINT</td>
    <td valign="top">19</td>
    <td valign="top">VARBINARY*</td>
  </tr>
  <tr>
    <td valign="top">10</td>
    <td valign="top">UINT4</td>
    <td valign="top">20</td>
    <td valign="top">FIXEDBINARY**</td>
  </tr>
  <tr>
    <td valign="top"></td>
    <td valign="top"></td>
    <td valign="top">21</td>
    <td valign="top">NULL (lowest)</td>
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
    <td valign="top">From:</td>
    <td valign="top">SMALLINT</td>
    <td valign="top">INT</td>
    <td valign="top">BIGINT</td>
    <td valign="top">DECIMAL</td>
    <td valign="top">FLOAT</td>
    <td valign="top">CHAR</td>
    <td valign="top">FIXEDBINARY</td>
    <td valign="top">VARCHAR</td>
    <td valign="top">VARBINARY</td>
  </tr>
  <tr>
    <td valign="top">SMALLINT*</td>
    <td valign="top"></td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
  </tr>
  <tr>
    <td valign="top">INT</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
  </tr>
  <tr>
    <td valign="top">BIGINT</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
  </tr>
  <tr>
    <td valign="top">DECIMAL</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
  </tr>
  <tr>
    <td valign="top">DOUBLE</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
  </tr>
  <tr>
    <td valign="top">FLOAT</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
  </tr>
  <tr>
    <td valign="top">CHAR</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
  </tr>
  <tr>
    <td valign="top">FIXEDBINARY**</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
    <td valign="top">no</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
  </tr>
  <tr>
    <td valign="top">VARCHAR***</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
    <td valign="top">yes</td>
  </tr>
  <tr>
    <td valign="top">VARBINARY**</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
    <td valign="top">yes</td>
    <td valign="top">yes</td>
    <td valign="top">no</td>
  </tr>
</table>
\* Not supported in this release.   
\*\* Used to cast binary data coming to/from sources such as MapR-DB/HBase.   
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
    <td valign="top">From:</td>
    <td valign="top">DATE</td>
    <td valign="top">TIME</td>
    <td valign="top">TIMESTAMP</td>
    <td valign="top">INTERVALYEAR</td>
    <td valign="top">INTERVALDAY</td>
  </tr>
  <tr>
    <td valign="top">CHAR</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
  </tr>
  <tr>
    <td valign="top">FIXEDBINARY*</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
    <td valign="top">Yes</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
  </tr>
  <tr>
    <td valign="top">VARCHAR</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
  </tr>
  <tr>
    <td valign="top">VARBINARY*</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
  </tr>
  <tr>
    <td valign="top">DATE</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
  </tr>
  <tr>
    <td valign="top">TIME</td>
    <td valign="top">No</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
  </tr>
  <tr>
    <td valign="top">TIMESTAMP</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
    <td valign="top">No</td>
  </tr>
  <tr>
    <td valign="top">INTERVALYEAR</td>
    <td valign="top">Yes</td>
    <td valign="top">No</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">No</td>
    <td valign="top">Yes</td>
  </tr>
  <tr>
    <td valign="top">INTERVALDAY</td>
    <td valign="top">Yes</td>
    <td valign="top">No</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">Yes</td>
    <td valign="top">No</td>
  </tr>
</table>
\* Used to cast binary data coming to/from sources such as MapR-DB/HBase.   

## CONVERT_TO and CONVERT_FROM Data Types

You use the CONVERT_TO and CONVERT_FROM data types as arguments to the CONVERT_TO
and CONVERT_FROM functions. CONVERT_FROM and CONVERT_TO methods transform a known binary representation/encoding to a Drill internal format. 

We recommend storing HBase/MapR-DB data in a binary representation rather than
a string representation. Use the \*\_BE types to store integer data types in an HBase or Mapr-DB table.  INT is a 4-byte little endian signed integer. INT_BE is a 4-byte big endian signed integer. The comparison order of \*\_BE encoded bytes is the same as the integer value itself if the bytes are unsigned or positive. Using a *_BE type facilitates scan range pruning and filter pushdown into HBase scan. 

\*\_HADOOPV in the data type name denotes the variable length integer as defined by Hadoop libraries. Use a \*\_HADOOPV type if user data is encoded in this format by a Hadoop tool outside MapR.

The following table lists the data types for use with the CONVERT_TO
and CONVERT_FROM functions:

**Type**| **Input Type**| **Output Type**  
---|---|---  
JSON | bytes | varchar
BOOLEAN_BYTE| bytes(1)| BOOLEAN  
TINYINT_BE| bytes(1)| TINYINT  
TINYINT| bytes(1)| TINYINT  
SMALLINT_BE| bytes(2)| SMALLINT  
SMALLINT| bytes(2)| SMALLINT  
INT_BE| bytes(4)| INT  
INT| bytes(4)| INT  
BIGINT_BE| bytes(8)| BIGINT  
BIGINT| bytes(8)| BIGINT  
FLOAT| bytes(4)| FLOAT (float4)  
DOUBLE| bytes(8)| DOUBLE (float8)  
INT_HADOOPV| bytes(1-9)| INT  
BIGINT_HADOOPV| BYTES(1-9)| BIGINT  
DATE_EPOCH_BE| bytes(8)| DATE  
DATE_EPOCH| bytes(8)| DATE  
TIME_EPOCH_BE| bytes(8)| TIME  
TIME_EPOCH| bytes(8)| TIME  
UTF8| bytes| VARCHAR  
UTF16| bytes| VAR16CHAR  
UINT8| bytes(8)| UINT8  

If you are unsure that the size of the source and destination INT or BIGINT you are converting is the same, use CAST to convert these data types to/from binary.


