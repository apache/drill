---
title: "Handling Different Data Types"
date: 2018-06-27 01:59:34 UTC
parent: "Data Types"
---
## Handling HBase Data
To query HBase data using Drill, convert every column of an HBase table to/from byte arrays from/to an SQL data type as described in the section ["Querying HBase"]({{ site.baseurl}}/docs/querying-hbase/). Use [CONVERT_TO or CONVERT_FROM]({{ site.baseurl }}/docs//data-type-conversion/#convert_to-and-convert_from) functions to perform conversions of HBase data.

## Handling Textual Data
In a textual file, such as CSV, Drill interprets every field as a VARCHAR, as previously mentioned. In addition to using the CAST function, you can also use TO_CHAR, TO_DATE, TO_NUMBER, and TO_TIMESTAMP. If the SELECT statement includes a WHERE clause that compares a column of an unknown data type, you might need to cast both the value of the column and the comparison value in the WHERE clause. In some cases, Drill performs implicit casting and no casting is necessary on your part.

## Handling JSON and Parquet Data
Complex and nested data structures in JSON and Parquet files are [composite types](({{site.baseurl}}/docs/supported-data-types/#composite-types)): map and array.

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
  

## Reading numbers of different types from JSON

The `store.json.read_numbers_as_double` and `store.json.all_text_mode` system/session options control how Drill implicitly casts JSON data. By default, when reading numerical values from a JSON file, Drill implicitly casts a number to the DOUBLE or BIGINT type depending on the presence or absence of a decimal point. If some numbers in a JSON map or array appear with and without a decimal point, such as 0 and 0.0, Drill throws a schema change error. By default, Drill reads numbers without decimal point as BIGINT values. The range of BIGINT is -9223372036854775808 to 9223372036854775807. A BIGINT result outside this range produces an error. 

To prevent Drill from attempting to read such data, set `store.json.read_numbers_as_double` or `store.json.all_text_mode` to true. Using `store.json.all_text_mode` set to true, Drill implicitly casts JSON data to VARCHAR. You need to cast the VARCHAR values to other types. Using `store.json.read_numbers_as_double` set to true, Drill implicitly casts numbers in the JSON file to DOUBLE. You need to cast the DOUBLE type to other types, such as FLOAT and INTEGER. Using `store.json.read_numbers_as_double` typically involves less explicit casting than using `store.json.all_text_mode` because you can often use the numerical data as is (DOUBLE).

## Guidelines for Using Float and Double

FLOAT and DOUBLE yield approximate results. These are variable-precision numeric types. Drill does not cast/convert all values precisely to the internal format, but instead stores approximations. Slight differences can occur in the value stored and retrieved. The following guidelines are recommended:

* For conversions involving monetary calculations, for example, that require precise results use the DECIMAL type instead of FLOAT or DOUBLE. Starting in Drill 1.14, the DECIMAL data type is enabled by default. 
* For complex calculations or mission-critical applications, especially those involving infinity and underflow situations, carefully consider the limitations of type casting that involves FLOAT or DOUBLE.
* Equality comparisons between floating-point values can produce unexpected results.

Values of FLOAT and DOUBLE that are less than the lowest value in the range (more negative) cause an error. Rounding can occur if the precision of an input number is too high. 
