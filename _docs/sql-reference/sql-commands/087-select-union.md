---
title: "SELECT UNION"
parent: "SQL Commands"
---
The UNION set operator returns all rows in the result sets of two separate query expressions. For example, if two employee tables exist, you can use the UNION set operator to merge the two tables and build a complete list of all the employees. Drill supports UNION ALL only. Drill does not support DISTINCT.


## Syntax
The UNION set operator supports the following syntax:

       query
       { UNION [ ALL ] }
       query
  

## Parameters  
*query*  

Any SELECT query that Drill supports. See SELECT.

## Usage Notes
   * The two SELECT query expressions that represent the direct operands of the UNION must produce the same number of columns. Corresponding columns must contain compatible data types. See Supported Data Types.  
   * Multiple UNION operators in the same SELECT statement are evaluated left to right, unless otherwise indicated by parentheses.  
   * You cannot use * in UNION ALL for schemaless data.

## Examples
The following example uses the UNION ALL set operator to combine click activity data before and after a marketing campaign. The data in the example exists in the `dfs.clicks workspace`.
 
       0: jdbc:drill:> select t.trans_id transaction, t.user_info.cust_id customer from `clicks/clicks.campaign.json` t 
       union all 
       select u.trans_id, u.user_info.cust_id  from `clicks/clicks.json` u limit 5;
       +-------------+------------+
       | transaction |  customer  |
       +-------------+------------+
       | 35232       | 18520      |
       | 31995       | 17182      |
       | 35760       | 18228      |
       | 37090       | 17015      |
       | 37838       | 18737      |
       +-------------+------------+

This UNION ALL query returns rows that exist in two files (and includes any duplicate rows from those files): `clicks.campaign.json` and `clicks.json`