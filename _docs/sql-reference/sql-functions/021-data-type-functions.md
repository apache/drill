---
title: "Data Type Functions"
date: 2018-06-20 17:37:51 UTC
parent: "SQL Functions"
---

Starting in Drill 1.14, Drill supports the following functions that return data type information:  

**sqlTypeOf()**  
Returns the data type of a column (using the SQL names) whether the column is NULL or not. You can use the SQL name in a CAST statement, for example:  

              sqlTypeOf( CAST(x AS <data type> ))  
              //Returns <data type> as the type name.  
If the type is DECIMAL, the type also includes precision and scale, for example:  
 
              DECIMAL(6, 3)  
**modeOf()**  
Returns the cardinality (mode) of the column as "NOT NULL", "NULLABLE", or "ARRAY". Drill data types include a cardinality, for example `Optional Int` or `Required VarChar`.  
 
**drillTypeOf()**  
Similar to typeOf(), but returns the internal Drill names even if the value is NULL.  
_____


### Usage Notes  

The data type functions are useful for data conversions. For example, if you know a column value is `Nullable Int`, you can assume that the data type is one that Drill derived. You can then merge the Drill-derived data type with the data type from another file that has actual values.  

____  


### Usage Examples

The follow examples show you how you can use the data type functions:  

**Example 1**  
This example shows the Drill internal type, nullable int, for a missing column:  

       SELECT sqlTypeOf(a) AS a_type, modeOf(a) AS a_mode FROM `json/all-null.json`;
       
       +----------+-----------+
       |  a_type  |  a_mode   |
       +----------+-----------+
       | INTEGER  | NULLABLE  |
       +----------+-----------+

**Example 2**  
This example shows arrays (repeated) types:  

       SELECT sqlTypeOf(columns) as col_type, modeOf(columns) as col_mode
       FROM `csv/cust.csv`;
       
       +--------------------+-----------+
       |      col_type      | col_mode  |
       +--------------------+-----------+
       | CHARACTER VARYING  | ARRAY     |
       +--------------------+-----------+
       Example 3: This example shows non-null types:
       SELECT sqlTypeOf(`name`) AS name_type, 
       modeOf(`name`) AS name_mode FROM `csvh/cust.csvh`;
       
       +--------------------+------------+
       |     name_type      | name_mode  |
       +--------------------+------------+
       | CHARACTER VARYING  | NOT NULL   |
       +--------------------+------------+

