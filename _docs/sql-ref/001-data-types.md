---
title: "Data Type Casting"
parent: "SQL Reference"
---
The following table lists the type arguments you can use with the cast function:

<table>
  <tr>
    <th>SQL Type</th>
    <th>Drill Description</th>
  </tr>
  <tr>
    <td>BIGINT</td>
    <td>8-byte signed integer</td>
  </tr>
  <tr>
    <td>BOOLEAN</td>
    <td>True or false</td>
  </tr>
  <tr>
    <td>DATE</td>
    <td>Years, months, and days in YYYY-­MM-­DD format</td>
  </tr>
  <tr>
    <td>NUMERIC, DECIMAL, or DEC(p,s)</td>
    <td>38-digit precision number. Same as numeric(p,s) where precision is p, and scale is s. Example: decimal(6,2) has 4 digits before the decimal point and 2 digits after the decimal point.</td>
  </tr>
  <tr>
    <td>FLOAT</td>
    <td>4-byte single precision floating point number</td>
  </tr>
  <tr>
    <td>DOUBLE, DOUBLE PRECISION</td>
    <td>8-byte double precision floating point number. </td>
  </tr>
  <tr>
    <td>INTEGER, INT</td>
    <td>4-byte signed integer</td>
  </tr>
  <tr>
    <td>INTERVAL</td>
    <td>Integer fields representing a period of time in years, months, day,s hours, minutes, seconds and optional milliseconds using ISO 8601 format.</td>
  </tr>
  <tr>
    <td>INTERVALDAY</td>
    <td>A simple version of the interval type expressing a period of time in days, hours, minutes, and seconds only.</td>
  </tr>
  <tr>
    <td>INTERVALYEAR</td>
    <td>A simple version of interval representing a period of time in years and months only.</td>
  </tr>
  <tr>
    <td>SMALLINT</td>
    <td>2-byte signed integer. Supported in Drill 0.9 and later./td>
  </tr>
  <tr>
    <td>TIME</td>
    <td>Hours, minutes, seconds in the form HH:mm:ss, 24-hour based</td>
  </tr>
  <tr>
    <td>TIMESTAMP</td>
    <td>JDBC timestamp in year, month, date hour, minute, second, and optional milliseconds format: yyyy-MM-dd HH:mm:ss.SSS</td>
  </tr>
  <tr>
    <td>CHARACTER VARYING, CHARACTER, CHAR, or VARCHAR</td>
    <td>Character string variable length. </td>
  </tr>
</table>

DATE, TIME, and TIMESTAMP store values in Coordinated Universal Time (UTC). Currently, Drill does not support casting a TIMESTAMP with time zone, but you can use the TO_TIMESTAMP function (link to example) in a query to use time stamp data having a time zone.

## Compatibility with Data Sources

The following sections describe the data type mapping between Drill and supported data sources.

* HBase  
  None. You need to convert data as shown in ["Querying HBase."](/docs/querying-hbase/).
* Hive  
  ["Hive-to-Drill Data Type Mapping"](/docs/hive-to-drill-data-type-mapping).
* JSON  
  [SQL-JSON data type mapping](/docs/json-data-model#data-type-mapping) or to varchar in all text mode
* MapR-DB  
  [The maprdb format](/docs/mapr-db-format) for reading (only).
* Parquet  
  [SQL Data Types to Parquet](/docs/parquet-format/sql-data-types-to-parquet). 
* Text: CSV, TSV, and other text  
  Implicitly casts all textual data to VARCHAR. 

Depending on the data format, you might need to [cast or convert](/docs/handling-disparate-data-types) data types to/from these SQL types when Drill reads/writes data.

## Guidelines for Using Float and Double

FLOAT and DOUBLE yield approximate results. These are variable-precision numeric types. Drill does not cast/convert all values precisely to the internal format, but instead stores approximations. Slight differences can occur in the value stored and retrieved. The following guidelines are recommended:

* For conversions involving monetary calculations, for example, that require precise results use the decimal type instead of float or double.
* For complex calculations or mission-critical applications, especially those involving infinity and underflow situations, carefully consider the limitations of type casting that involves FLOAT or DOUBLE.
* Equality comparisons between floating-point values can produce unexpected results.

Values of FLOAT and DOUBLE that are less than the lowest value in the range (more negative) cause an error. Rounding can occur if the precision of an input number is too high. 

## Complex Data Types

Drill extends SQL to supoort complex and nested data structures in JSON and Parquet files. Drill reads/writes maps and arrays from/to JSON and Parquet files.  

* A map is a set of name/value pairs. 
  A value in a map can be a scalar type, such as string or int, or a complex type, such as an array or another map. 
* An array is a repeated list of values.
  A value in an array can be a scalar type, such as string or int, or an array can be a complex type, such as a map or another array.

In Drill, you do not cast a map or array to another type. 

The following example shows a JSON map having scalar values:

    phoneNumber: 
    { 
      areaCode: 622, 
      number: 1567845
    }

The following example shows a JSON map having an array as a value:

    { citiesLived : 
      [ 
        { place : Los Angeles,       
          yearsLived : [ 1989, 1993, 1998, 2002]     
        } 
      ] 
    }

The following example shows a JSON array having scalar values:

    yearsLived: [1990, 1993, 1998, 2008]

The following example shows a JSON array having complex type values:

    children: 
      [ 
        { age : 10,
          gender : Male,
          name : Earl
        },
        { age : 6,
          gender : Male,
          name : Sam,
        { age : 8,
          gender : Male,
          name : Kit
        }
      ]
  
