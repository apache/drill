---
title: "Develop Custom Functions Introduction"
parent: "Develop Custom Functions"
---
Drill provides a high performance Java API with interfaces that you can
implement to develop simple custom functions. Custom functions
are reusable SQL functions that you develop in Java to encapsulate code that
processes column values during a query. Custom functions have all the performance of the Drill primitive operations. Custom functions can perform
calculations and transformations that built-in SQL operators and functions do
not provide. Custom functions are called from within a SQL statement, like a
regular function, and return a single value.

This section includes a [tutorial]({{site.baseurl}}/docs/tutorial-develop-a-simple-function/) for creating a simple function that is based on a github project, which you can download. 

## Simple Function

A simple function operates on a single row and produces a single row as the
output. When you include a simple function in a query, the function is called
once for each row in the result set. Mathematical and string functions are
examples of simple functions. 

## Aggregate Function

The API for developing aggregate custom functions is at the alpha stage and intended for experimental use only. Aggregate functions differ from simple functions in the number of rows that
they accept as input. An aggregate function operates on multiple input rows
and produces a single row as output. The COUNT(), MAX(), SUM(), and AVG()
functions are examples of aggregate functions. You can use an aggregate
function in a query with a GROUP BY clause to produce a result set with a
separate aggregate value for each combination of values from the GROUP BY
clause.

## Process

To develop custom functions that you can use in your Drill queries, you must
complete the following tasks:

  1. Create a Java program that implements Drill’s simple or aggregate interface, and compile a sources and a classes JAR file.
  2. Add the sources and classes JAR files to Drill’s classpath.
  3. Add the name of the package that contains the classes to Drill’s main configuration file, drill-override.conf. 