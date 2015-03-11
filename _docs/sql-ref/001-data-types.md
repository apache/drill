---
title: "Data Types"
parent: "SQL Reference"
---
You can use the following SQL data types in Drill queries:

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
    <td>TRUE (1) or FALSE (0)</td>
  </tr>
  <tr>
    <td>DATE</td>
    <td>Years months and days in the form in the form YYYY-­MM-­DD</td>
  </tr>
  <tr>
    <td>DECIMAL</td>
    <td>38-digit precision</td>
  </tr>
  <tr>
    <td>FLOAT</td>
    <td>4-byte single precision floating point number</td>
  </tr>
  <tr>
    <td>DOUBLE</td>
    <td>8-byte double precision floating point number</td>
  </tr>
  <tr>
    <td>INTEGER</td>
    <td>4-byte signed integer</td>
  </tr>
  <tr>
    <td>INTERVAL</td>
    <td>Integer fields representing a period of time depending on the type of interval</td>
  </tr>
  <tr>
    <td>INTERVALDAY</td>
    <td>Integer fields representing a day</td>
  </tr>
  <tr>
    <td>INTERVALYEAR</td>
    <td>Integer fields representing a year</td>
  </tr>
  <tr>
    <td>SMALLINT</td>
    <td>2-byte signed integer. Supported in Drill 0.9 and later.</td>
  </tr>
  <tr>
    <td>TIME</td>
    <td>Hours minutes seconds 24-hour basis</td>
  </tr>
  <tr>
    <td>TIMESTAMP</td>
    <td>Conventional UNIX Epoch timestamp.</td>
  </tr>
  <tr>
    <td>VARCHAR</td>
    <td>Character string variable length</td>
  </tr>
</table>

## Complex Data Types

Complex and nested data structures in JSON and Parquet files are of map and array types. 

* A map is a set of name/value pairs. 
  A value in a map can be a scalar type, such as string or int, or a complex type, such as an array or another map. 
* An array is a repeated list of values.
  A value in an array can be a scalar type, such as string or int, or an array can be a complex type, such as a map or another array.

Drill reads/writes maps and arrays from/to JSON and Parquet files. In Drill, you do not cast a map or array to another type. 


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
  
