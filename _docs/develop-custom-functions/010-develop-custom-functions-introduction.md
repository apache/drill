---
title: "Develop Custom Functions Introduction"
date: 2018-06-27 20:52:59 UTC
parent: "Develop Custom Functions"
---
Drill provides a high performance Java API with interfaces that you can use to develop simple and aggregate custom functions. Custom functions are reusable SQL functions that you develop in Java to encapsulate code that processes column values during a query. 

Custom functions are called from within a SQL statement, like a regular function, and return a single value. Custom functions perform like Drill primitive operations. They can perform calculations and transformations that built-in SQL operators and functions do not provide.  

## Simple Function

A simple function operates on a single row and produces a single row as the
output. When you include a simple function in a query, the function is called
once for each row in the result set. Mathematical and string functions are
examples of simple functions.  

You can use the provided [tutorial]({{site.baseurl}}/docs/tutorial-develop-a-simple-function/) to create a simple function that is based on a github project, which you can download.

## Aggregate Function

The API for developing aggregate custom functions is at the alpha stage and intended for experimental use only. Aggregate functions differ from simple functions in the number of rows that they accept as input. An aggregate function operates on multiple input rows
and produces a single row as output.  

The COUNT(), MAX(), SUM(), and AVG() functions are examples of aggregate functions. You can use an aggregate function in a query with a GROUP BY clause to produce a result set with a
separate aggregate value for each combination of values from the GROUP BY clause.

## Development Process
To develop custom functions for Drill, create a Java program that implements Drill’s [simple]({{site.baseurl}}/docs/developing-a-simple-function/) or [aggregate]({{site.baseurl}}/docs/developing-an-aggregate-function/) interface and then add your custom function(s) to Drill.  
  
As of Drill 1.9, there are two methods for adding custom functions to Drill. Administrators can manually add custom functions to Drill, or users can issue the CREATE FUNCTION USING JAR command to register their custom functions. The CREATE FUNCTION USING JAR command is part of the [Dynamic UDF feature]({{site.baseurl}}/docs/dynamic-udfs/) which requires assistance from an administrator.   

**Note:** Starting in Drill 1.14, Drill supports vardecimal as the decimal data type. For custom functions that return values with a decimal data type, you must specify an appropriate FunctionTemplate.ReturnType in the FunctionTemplate. Also, verify the result of functions in queries that have a LIMIT 0 clause when LIMIT 0 optimization is enabled. 
