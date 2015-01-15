---
title: "Query Data"
parent: "Apache Drill Documentation"
---
You can query local and distributed file systems, Hive, and HBase data sources
registered with Drill. If you connected directly to a particular schema when
you invoked SQLLine, you can issue SQL queries against that schema. If you did
not indicate a schema when you invoked SQLLine, you can issue the `USE
<schema>` statement to run your queries against a particular schema. After you
issue the `USE` statement, you can use absolute notation, such as
`schema.table.column`.

Click on any of the following links for information about various data source
queries and examples:

  * [Querying a File System](/confluence/display/DRILL/Querying+a+File+System)
  * [Querying HBase](/confluence/display/DRILL/Querying+HBase)
  * [Querying Hive](/confluence/display/DRILL/Querying+Hive)
  * [Querying Complex Data](/confluence/display/DRILL/Querying+Complex+Data)
  * [Querying the INFORMATION_SCHEMA](/confluence/display/DRILL/Querying+the+INFORMATION_SCHEMA)
  * [Querying System Tables](/confluence/display/DRILL/Querying+System+Tables)
  * [Drill Interfaces](/confluence/display/DRILL/Drill+Interfaces)

You may need to use casting functions in some queries. For example, you may
have to cast a string `"100"` to an integer in order to apply a math function
or an aggregate function.

You can use the EXPLAIN command to analyze errors and troubleshoot queries
that do not run. For example, if you run into a casting error, the query plan
text may help you isolate the problem.

    0: jdbc:drill:zk=local> !set maxwidth 10000
    0: jdbc:drill:zk=local> explain plan for select ... ;

The set command increases the default text display (number of characters). By
default, most of the plan output is hidden.

You may see errors if you try to use non-standard or unsupported SQL syntax in
a query.

Remember the following tips when querying data with Drill:

  * Include a semicolon at the end of SQL statements, except when you issue a command with an exclamation point `(!).   
`Example: `!set maxwidth 10000`

  * Use backticks around file and directory names that contain special characters and also around reserved words when you query a file system .   
The following special characters require backticks:

    * . (period)
    * / (forward slash)
    * _ (underscore)

Example: ``SELECT * FROM dfs.default.`sample_data/my_sample.json`; ``

  * `CAST` data to `VARCHAR` if an expression in a query returns `VARBINARY` as the result type in order to view the `VARBINARY` types as readable data. If you do not use the `CAST` function, Drill returns the results as byte data.  
Example: `CAST (VARBINARY_expr as VARCHAR(50))`

