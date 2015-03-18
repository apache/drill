---
title: "Data Types"
parent: "SQL Reference"
---
Depending on the data format, you might need to cast or convert data types when Drill reads/writes data.

After Drill reads schema-less data into SQL tables, you need to cast data types explicitly to query the data. In some cases, Drill converts schema-less data to typed data implicitly. In this case, you do not need to cast. The file format of the data and the nature of your query determines the requirement for casting or converting. 

Differences in casting depend on the data source. The following list describes how Drill treats data types from various data sources:

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


In general, Drill implicitly casts (promotes) one type to another type based on the order of precedence shown in the following table. Drill also considers the performance cost of implicit casting to one type versus another. Drill usually implicitly casts a type from a lower precedence to a type having higher precedence. For instance, NULL can be promoted to any other type; SMALLINT can be promoted into INT. INT is not promoted to SMALLINT due to possible precision loss.

Under certain circumstances, such as queries involving  substr and concat functions, Drill reverses the order of precedence and allows a cast to VARCHAR from a type of higher precedence than VARCHAR, such as BIGINT. 

The following table lists data types top to bottom, in descending precedence. Drill implicitly casts to more data types than are currently supported for explicit casting.

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
    <td>BIGINT</td>
  </tr>
  <tr>
    <td>2</td>
    <td>INTERVALYEAR</td>
    <td>14</td>
    <td>UINT4</td>
  </tr>
  <tr>
    <td>3</td>
    <td>INTERVLADAY</td>
    <td>15</td>
    <td>INT</td>
  </tr>
  <tr>
    <td>4</td>
    <td>TIMESTAMPTZ</td>
    <td>16</td>
    <td>UINT2</td>
  </tr>
  <tr>
    <td>5</td>
    <td>TIMETZ</td>
    <td>17</td>
    <td>SMALLINT</td>
  </tr>
  <tr>
    <td>6</td>
    <td>TIMESTAMP</td>
    <td>18</td>
    <td>UINT1</td>
  </tr>
  <tr>
    <td>7</td>
    <td>DATE</td>
    <td>19</td>
    <td>VAR16CHAR</td>
  </tr>
  <tr>
    <td>8</td>
    <td>TIME</td>
    <td>20</td>
    <td>FIXED16CHAR</td>
  </tr>
  <tr>
    <td>9</td>
    <td>FLOAT8</td>
    <td>21</td>
    <td>VARCHAR</td>
  </tr>
  <tr>
    <td>10</td>
    <td>DECIMAL</td>
    <td>22</td>
    <td>FIXEDCHAR</td>
  </tr>
  <tr>
    <td>11</td>
    <td>MONEY</td>
    <td>23</td>
    <td>VARBINARY</td>
  </tr>
  <tr>
    <td>12</td>
    <td>UINT8</td>
    <td>24</td>
    <td>FIXEDBINARY</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>25</td>
    <td>NULL</td>
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

The following tables show data types that Drill can cast to/from other data types. Not all types are available for explicit casting in the current release.

### Explicit type Casting: Numeric and Character types

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
    <td></td>
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

### Explicit Type Casting: Date/Time types

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
    <td>FIXEDCHAR</td>
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

### Using CONVERT_TO and CONVERT_FROM

To query HBase data in Drill, convert every column of an HBase table to/from byte arrays from/to an [SQL data type](/docs/data-types/) that Drill supports when writing/reading data. For examples of how to use these functions, see ["Convert and Cast Functions".](/docs/sql-functions#convert-and-cast-functions)

## Handling Textual Data
In a textual file, such as CSV, Drill interprets every field as a VARCHAR, as previously mentioned. In addition to using the CAST function, you can also use [to_char](link), [to_date](line), [to_number](link), and [to_timestamp](link). If the SELECT statement includes a WHERE clause that compares a column of an unknown data type, cast both the value of the column and the comparison value in the WHERE clause.
