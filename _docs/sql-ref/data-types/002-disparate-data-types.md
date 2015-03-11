---
title: "Handling Disparate Data Types"
parent: "Data Types"
---
[Previous](/docs/supported-date-time-data-type-formats)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Back to Table of Contents](/docs)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Next](/docs/lexical-structure)

The file format of the data and planned queries determine the casting or converting required, if any. When Drill reads schema-less data into SQL tables for querying, you might need to cast one data type to another explicitly. In some cases, Drill converts schema-less data to typed data implicitly. In this case, you do not need to cast. Drill does not implicitly cast HBase binary data. You use convert_to and convert_from functions to work with HBase data in Drill.

With respect to data types, Drill treats data from these sources as follows:

* HBase
  No implicit casting to SQL types. Convert data to appropriate types as shown in ["Querying HBase."](/docs/querying-hbase/)
* Hive
  Implicitly casts Hive types to SQL types as shown in the Hive [type mapping example](/docs/hive-to-drill-data-type-mapping#type-mapping-example)
* JSON
  Implicitly casts JSON data to SQL types as shown in the [SQL and JSON type mapping table](/docs/json-data-model#data-type-mapping) of the JSON Data Model documentation.
* MapR-DB
  Implicitly casts MapR-DB data to SQL types when you use the maprdb format for reading MapR-DB data. The dfs storage plugin defines the format when you install Drill from the mapr-drill package on a MapR node.
* Parquet
  Implicitly casts JSON data to the SQL types shown in [SQL Data Types to Parquet](/docs/parquet-format/sql-data-types-to-parquet). 
* Text: CSV, TSV, and other text
  Implicitly casts all textual data to varchar. 

Drill supports a number of functions to cast and convert compatible data types:

* cast
  Casts textual data from one data type to another.
* convert_to and convert_from
  Converts binary data from one data type to another.
* to_char
  Converts a timestamp, interval, integer, real/double precision, or decimal to a string.
* to_date
  Converts a string to a date.
* to_number
  Converts a string to a decimal.
* to_timestamp
  Converts a string to a timestamp.

### Using Cast

Embed a cast function in a query using this syntax:

    cast <expression> AS <data type> 

* expression
  A entity that has single data value, such as a column name, of the data type you want to cast to a different type
* data type
  The target data type, such as INTEGER or DATE

Example: Inspect integer data and cast data to a decimal

    SELECT c_row, c_int FROM mydata WHERE c_row = 9;
    c_row | c_int
    ------+------------
        9 | -2147483648
    (1 row)

   SELECT c_row, cast(c_int as decimal(28,8)) FROM my_data WHERE c_row = 9;
    c_row | c_int
    ------+---------------------
    9     | -2147483648.00000000
    (1 row)

If the SELECT statement includes a WHERE clause that compares a column of an unknown data type, cast both the value of the column and the comparison value in the WHERE clause. For example:

    SELECT c_row, cast(c_int as decimal(28,8)) FROM mydata WHERE cast(c_int as decimal(28,8)) > -3.0

Although you can use cast to handle binary data, convert_to and convert_from are recommended for use with binary data.

### Using convert_to and convert_from

To query HBase data in Drill, convert every column of an HBase table to/from byte arrays from/to an [SQL data type](/docs/data-types/) that Drill supports when writing/reading data. For examples of how to use these functions, see ["Convert and Cast Functions".](/docs/sql-functions#convert-and-cast-functions).

## Handling Textual Data
In a textual file, such as CSV, Drill interprets every field as a varchar, as previously mentioned. In addition to using the cast function, you can also use [to_char](link), [to_date](line), [to_number](link), and [to_timestamp](link). If the SELECT statement includes a WHERE clause that compares a column of an unknown data type, cast both the value of the column and the comparison value in the WHERE clause.


