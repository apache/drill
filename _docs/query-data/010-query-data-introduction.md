---
title: "Query Data Introduction"
parent: "Query Data"
---
You can query local and distributed file systems, Hive, and HBase data sources
registered with Drill. You issue the `USE
<storage plugin>` statement to run your queries against a particular storage plugin. You use dot notation and back ticks to specify the storage plugin name and sometimes the workspace name. For example, to use the dfs storage plugin and default workspace, issue this command: ``USE dfs.`default``

Alternatively, you can omit the USE statement, and specify the storage plugin and workspace name using dot notation and back ticks. For example:

``dfs.`default`.`/Users/drill-user/apache-drill-1.0.0/log/sqlline_queries.json```;

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

Remember the following tips when querying data with Drill:

  * Include a semicolon at the end of SQL statements, except when you issue a command with an exclamation point `(!).   
    `Example: `!set maxwidth 10000`
  * Use backticks around file and directory names that contain special characters and also around reserved words when you query a file system.   
    The following special characters require backticks:

    * . (period)
    * / (forward slash)
    * _ (underscore)
    Example: ``SELECT * FROM dfs.default.`sample_data/my_sample.json`; ``
  * `CAST` data to `VARCHAR` if an expression in a query returns `VARBINARY` as the result type in order to view the `VARBINARY` types as readable data. If you do not use the `CAST` function, Drill returns the results as byte data.    
     Example: `CAST (VARBINARY_expr as VARCHAR(50))`
  * When selecting all (SELECT *) schema-less data, the order of returned columns might differ from the stored order and might vary from query to query.