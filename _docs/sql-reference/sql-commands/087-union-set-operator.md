---
title: "UNION Set Operator"
parent: "SQL Commands"
---
The UNION set operator combines the result sets of two separate query expressions. The result set of each query must have the same number of columns and compatible data types. UNION automatically removes duplicate records from the result set. UNION ALL returns all duplicate records.


## Syntax
The UNION set operator supports the following syntax:

       query
       { UNION [ ALL ] }
       query
  

## Parameters  
*query*  

Any SELECT query that Drill supports. See [SELECT]({{site.baseurl}}/docs/select/).

## Usage Notes
   * The two SELECT query expressions that represent the direct operands of the UNION must produce the same number of columns. Corresponding columns must contain compatible data types. See [Supported Data Types]({{site.baseurl}}/docs/supported-data-types/).  
   * Multiple UNION operators in the same SELECT statement are evaluated left to right, unless otherwise indicated by parentheses.  
   * You can only use * on either side of UNION when the data source has a defined schema, such as data in Hive or views.
   * You must explicitly specify columns.

## Example
The following example uses the UNION ALL set operator to combine click activity data before and after a marketing campaign. The data in the example exists in the `dfs.clicks workspace`.
 
       0: jdbc:drill:> SELECT t.trans_id transaction, t.user_info.cust_id customer 
       FROM `clicks/clicks.campaign.json` t 
       UNION ALL
       SELECT u.trans_id, u.user_info.cust_id FROM `clicks/clicks.json` u LIMIT 5;
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