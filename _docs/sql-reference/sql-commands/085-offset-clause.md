---
title: "OFFSET Clause"
date: 2018-11-02
parent: "SQL Commands"
---
The OFFSET clause provides a way to skip a specified number of first rows in a result set before starting to return any rows.

## Syntax
The OFFSET clause supports the following syntax:

       OFFSET start { ROW | ROWS }

## Parameters
*rows*  
Specifies the number of rows Drill should skip before returning the result set. 

## Usage Notes  
   * The OFFSET number must be a positive integer and cannot be larger than the number of rows in the underlying result set or no rows are returned.
   * You can use the OFFSET clause in conjunction with the LIMIT and ORDER BY clauses.
   * When used with the LIMIT option, OFFSET rows are skipped before starting to count the LIMIT rows that are returned. If the LIMIT option is not used, the number of rows in the result set is reduced by the number of rows that are skipped.
   * The rows skipped by an OFFSET clause still have to be scanned, so it might be inefficient to use a large OFFSET value.

## Examples
The following example query returns the result set from row 101 and on, skipping the first 100 rows of the table:

       SELECT * FROM dfs.logs OFFSET 100 ROWS; 

