---
title: "Handling Different Data Types"
parent: "Data Types"
---
## Handling HBase Data
To query HBase data in Drill, convert every column of an HBase table to/from byte arrays from/to an SQL data type using CONVERT_TO or CONVERT_FROM. For examples of how to use these functions, see "Convert and Cast Functions".

## Handling Textual Data
In a textual file, such as CSV, Drill interprets every field as a VARCHAR, as previously mentioned. In addition to using the CAST function, you can also use TO_CHAR, TO_DATE, TO_NUMBER, and TO_TIMESTAMP. If the SELECT statement includes a WHERE clause that compares a column of an unknown data type, cast both the value of the column and the comparison value in the WHERE clause.

## Handling JSON and Parquet Data
Complex and nested data structures in JSON and Parquet files are of map and array types.

A map is a set of name/value pairs. A value in a map can be a scalar type, such as string or int, or a complex type, such as an array or another map.
An array is a repeated list of values. A value in an array can be a scalar type, such as string or int, or an array can be a complex type, such as a map or another array.

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
  

## All text mode option
All text mode is a system option for controlling how Drill implicitly casts JSON data. When reading numerical values from a JSON file, Drill implicitly casts a number to the DOUBLE or BIGINT type depending on the presence or absence a decimal point. If some numbers in a JSON map or array appear with and without a decimal point, such as 0 and 0.0, Drill throws a schema change error. To prevent Drill from attempting to read such data, [set all_text_mode](/docs/json-data-model#handling-type-differences) to true. In all text mode, Drill implicitly casts JSON data to VARCHAR, which you can subsequently cast to desired types.

Drill reads numbers without decimal point as BIGINT values by default. The range of BIGINT is -9223372036854775808 to 9223372036854775807. A BIGINT result outside this range produces an error. Use `all_text_mode` to select data as VARCHAR and then cast the data to a numerical type.

## Guidelines for Using Float and Double

FLOAT and DOUBLE yield approximate results. These are variable-precision numeric types. Drill does not cast/convert all values precisely to the internal format, but instead stores approximations. Slight differences can occur in the value stored and retrieved. The following guidelines are recommended:

* For conversions involving monetary calculations, for example, that require precise results use the decimal type instead of float or double.
* For complex calculations or mission-critical applications, especially those involving infinity and underflow situations, carefully consider the limitations of type casting that involves FLOAT or DOUBLE.
* Equality comparisons between floating-point values can produce unexpected results.

Values of FLOAT and DOUBLE that are less than the lowest value in the range (more negative) cause an error. Rounding can occur if the precision of an input number is too high. 