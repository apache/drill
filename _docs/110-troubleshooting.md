---
title: "Troubleshooting"
date: 2017-02-24 22:12:36 UTC
---

You may experience certain known issues when using Drill. This document lists some known issues and resolutions for each.

## Before You Begin
Before you begin troubleshooting issues that you encounter in Drill, make sure you know which Drillbit is acting as the Foreman in the Drill cluster. The Drill node to which a client or application connects is the Foreman. 

You should also know the version of Drill running in the cluster. You can search JIRA for issues related to the version to see if a patch or workaround exists for the issue that you encountered.

### Identify the Foreman
Issue the following query to identify the Foreman node:  

``SELECT hostname FROM sys.drillbits WHERE `current` = true;``

### Identify the Drill Version
Issue the following query to identify the version of Drill running in your cluster:  
`SELECT version FROM sys.version;`

### Enable Verbose Errors
You can enable the verbose errors option for a more detailed error print-out.

Issue the following command to enable the verbose errors option:  

``ALTER SESSION SET `exec.errors.verbose` = true``

## Troubleshooting Problems and Solutions
If you have any of the following problems, try the suggested solution:

* [Memory Issues]({{site.baseurl}}/docs/troubleshooting/#memory-issues)
* [Query Parsing Errors]({{site.baseurl}}/docs/troubleshooting/#query-parsing-errors)
* [Query Parsing Errors Caused by Reserved Words]({{site.baseurl}}/docs/troubleshooting/#query-parsing-errors-caused-by-reserved-words)
* [Table Not Found]({{site.baseurl}}/docs/troubleshooting/#table-not-found)
* [Access Nested Fields without Table Name/Alias]({{site.baseurl}}/docs/troubleshooting/#access-nested-fields-without-table-name-alias)
* [Unexpected Null Values for Columns in Results]({{site.baseurl}}/docs/troubleshooting/#unexpected-null-values-for-columns-in-results)
* [Using Functions with Incorrect Data Types]({{site.baseurl}}/docs/troubleshooting/#using-functions-with-incorrect-data-types)
* [Query Takes a Long Time to Return]({{site.baseurl}}/docs/troubleshooting/#query-takes-a-long-time-to-return)
* [Schema Changes]({{site.baseurl}}/docs/troubleshooting/#schema-changes)
* [Timestamps and Timezones Other Than UTC]({{site.baseurl}}/docs/troubleshooting/#timestamps-and-timezones-other-than-utc)
* [Unexpected ODBC Issues]({{site.baseurl}}/docs/troubleshooting/#unexpected-odbc-issues)
* [JDBC/ODBC Connection Issues with ZooKeeper]({{site.baseurl}}/docs/troubleshooting/#jdbc-odbc-connection-issues-with-zookeeper)
* [Metadata Queries Take a Long Time to Return]({{site.baseurl}}/docs/troubleshooting/#metadata-queries-take-a-long-time-to-return)
* [Unexpected Results Due to Implicit Casting]({{site.baseurl}}/docs/troubleshooting/#unexpected-results-due-to-implicit-casting)
* [Column Alias Causes an Error]({{site.baseurl}}/docs/troubleshooting/#column-alias-causes-an-error)
* [List (Array) Contains Null]({{site.baseurl}}/docs/troubleshooting/#list-array-contains-null)
* [SELECT COUNT (*) Takes a Long Time to Run]({{site.baseurl}}/docs/troubleshooting/#select-count-takes-a-long-time-to-run)
* [Tableau Issues]({{site.baseurl}}/docs/troubleshooting/#tableau-issues)
* [GROUP BY Using Alias]({{site.baseurl}}/docs/troubleshooting/#group-by-using-alias)
* [Casting a VARCHAR String to an INTEGER Causes an Error]({{site.baseurl}}/docs/troubleshooting/#casting-a-varchar-string-to-an-integer-causes-an-error)
* [Unexpected Exception during Fragment Initialization]({{site.baseurl}}/docs/troubleshooting/#unexpected-exception-during-fragment-initialization)
* [Queries Running Out of Memory]({{site.baseurl}}/docs/troubleshooting/#queries-running-out-of-memory)
* [Unclear Error Message]({{site.baseurl}}/docs/troubleshooting/#unclear-error-message)
* [Error Starting Drill in Embedded Mode]({{site.baseurl}}/docs/troubleshooting/#error-starting-drill-in-embedded-mode)

### Memory Issues
Symptom: Memory problems occur when you run certain queries, such as those with sort operators.

Solution: Increase the value of the [`planner.memory.max_query_memory_per_node`]({{site.baseurl}}/docs/configuration-options-introduction/#system-options) option, which sets the maximum amount of direct memory allocated to the sort operator in each query on a node. If a query plan contains multiple sort operators, they all share this memory. 
If you continue to encounter memory issues after increasing the  `planner.memory.max_query_memory_per_node` value, you can also reduce the value of the `planner.width.max_per_node` option to reduce the level of parallelism per node. However, this may increase the amount of time required for a query to complete.
See [Configuring Drill Memory]({{site.baseurl}}/docs/configuring-drill-memory/).


### Query Parsing Errors
Symptom:  

`PARSE ERROR: At line x, column x: ...`  
Solution: Verify that you are using valid syntax. See [SQL Reference]({{ site.baseurl }}/docs/sql-reference-introduction/).


### Query Parsing Errors Caused by Reserved Words
Using a common word, such as count, which is a reserved word, as an identifier causes an error.  
Symptom:   

       SELECT COUNT FROM dfs.tmp.`test2.json`;
       Query failed: SYSTEM ERROR: Failure parsing SQL. Encountered "count from" at line 1, column 8.
       Was expecting one of:
           "UNION" ...
           "INTERSECT" ...
Solution: Enclose keywords and [identifiers that SQL cannot parse]({{site.baseurl}}/docs/lexical-structure/#identifier) in back ticks. See [Reserved Keywords]({{ site.baseurl }}/docs/reserved-keywords/).

``SELECT `count` FROM dfs.tmp.`test2.json```;

### Table Not Found
To resolve a Table Not Found problem that results from querying a file, try the solutions listed in this section. To resolve a Table Not Found problem that results from querying a directory, try removing or moving hidden files from the directory. Drill 1.1 and earlier might create hidden files in a directory ([DRILL-2424](https://issues.apache.org/jira/browse/DRILL-2424)). 

Symptom that results from querying a file:
 
       SELECT * FROM dfs.drill.test2.json;
       Query failed: PARSE ERROR: From line 1, column 15 to line 1, column 17: Table 'dfs.drill.test2.json' not found  

Solutions:

* Run SHOW FILES to list the files in the dfs.drill workspace. 
* Check the permission of the files with those for the the Drill user.  
* Enclose file and path name in back ticks:  
  ``SELECT * FROM dfs.drill.`test2.json`;``  
* Drill may not be able to determine the type of file you are trying to read. Try using Drill [Default Input Format]({{site.baseurl}}/docs/plugin-configuration-basics/#storage-plugin-attributes).  
* Verify that your storage plugin is correctly configured.
* Verify that Drill can auto-detect your file format.  Drill supports auto-detection for the following formats:  
  * CSV
  * TSV
  * PSV
  * Parquet
  * JSON

### Access Nested Fields without Table Name/Alias
Symptom: 

       SELECT x.y …  
       PARSE ERROR: At line 1, column 8: Table 'x' not found  
Solution: Add table name or alias to the field reference:  

`SELECT t.x.y FROM t`  

### Unexpected Null Values for Columns in Results
Symptom:  The following type of query returns NULL values:  

`SELECT t.price FROM t` 


Solution: Drill is schema-less system. Verify that column names are typed correctly.


### Using Functions with Incorrect Data Types

Symptom: Example  

       SELECT TRUNC(c3) FROM t3;
       
       0: jdbc:drill:schema=dfs> select trunc(c3) from t3;
       Query failed: SYSTEM ERROR: Failure while trying to materialize incoming schema.  Errors:
        
       Error in expression at index -1.  Error: Missing function implementation: [trunc(DATE-OPTIONAL)].  Full expression: --UNKNOWN EXPRESSION--..
       
       Fragment 0:0
       
       [6e465594-4d83-4042-b88d-50e7eb207484 on atsqa4-133.qa.lab:31010]
       Error: exception while executing query: Failure while executing query. (state=,code=0)  

Solution: Ensure that the function is invoked with the correct data type parameters. In this example, c3 is an unsupported date type. 

### Query Takes a Long Time to Return

Symptom: Query takes longer to return than expected.

Solution: Review the [query profile]({{ site.baseurl }}/docs/query-profiles/) and perform the following tasks:  

 * Determine whether progress is being made. Look at last update and last change times.
 * Look at where Drill is currently spending time and try to optimize those operations.
 * Confirm that Drill is taking advantage of the nature of your data, including things like partition pruning and projection pushdown.

### Schema Changes

Symptom:  

       DATA_READ ERROR: Error parsing JSON - You tried to write a XXXX type when you are using a ValueWriter of type XXXX.       
       File  /src/data/schema.json
       Record  2
       Fragment 0:0  

Solution: Drill does not fully support schema changes. Either ensure that your schemas are the same or only select columns that share schema.

### Timestamps and Timezones Other Than UTC

Symptoms: Issues with timestamp and timezone. Illegal instant due to time zone offset transition (America/New_York)

Solution: Convert data to UTC format. You are most likely trying to import date and time data that is encoded one timezone in a different timezone.  Drill’s default behavior is to use the system time for converting incoming data.  If you are providing UTC data and your Drillbit nodes do not run with UTC time, you’ll need to run your JVM with the following system property:

`-Duser.timezone=UTC`  

http://www.openkb.info/2015/05/understanding-drills-timestamp-and.html  

### Unexpected ODBC Issues

Symptom: ODBC errors.

Solution: Make sure that the ODBC driver version is compatible with the server version. [Driver installation instructions]({{site.baseurl}}/docs/installing-the-odbc-driver) include how to check the driver version. 
Turn on ODBC driver debug logging to better understand failure.  

### JDBC/ODBC Connection Issues with ZooKeeper

Symptom: Client cannot resolve ZooKeeper host names for JDBC/ODBC.

Solution: Ensure that Zookeeper is up and running. Verify that Drill has the correct `drill-override.conf` settings for the Zookeeper quorum.

### Metadata Queries Take a Long Time to Return

Symptom: Running SHOW databases/schemas/tables hangs. In general any INFORMATION_SCHEMA queries hang.

Solution: Disable incorrectly configured storage plugins or start appropriate services. Check compatibility matrix for the appropriate versions.  

### Unexpected Results Due to Implicit Casting

Symptom: Drill implicitly casts based on order of precedence.

Solution: Review Drill casting behaviors and explicitly cast for the expected results. See [Data Types]({{ site.baseurl }}/docs/handling-different-data-types/).

### Column Alias Causes an Error

Symptom: Drill is not case sensitive, and you can provide any alias for a column name. However, if the storage type is case sensitive, the alias name may conflict and cause errors.

Solution: Verify that the column alias does not conflict with the storage type. See [Lexical Structures]({{ site.baseurl }}/docs/lexical-structure/#case-sensitivity).  

### List (Array) Contains Null

Symptom: UNSUPPORTED\_OPERATION ERROR: Null values are not supported in lists by default. 

Solution: Avoid selecting fields that are arrays containing nulls. Change Drill session settings to enable all_text_mode. Set store.json.all\_text_mode to true, so Drill treats JSON null values as a string containing the word 'null'.

### SELECT COUNT (\*) Takes a Long Time to Run

Solution: In some cases, the underlying storage format does not have a built-in capability to return a count of records in a table.  In these cases, Drill does a full scan of the data to verify the number of records.

### Tableau Issues

Symptom: You see a lot of error messages in ODBC trace files or the performance is slow.

Solution: Verify that you have installed the TDC file shipped with the ODBC driver.  

### GROUP BY Using Alias

Symptom: Invalid column.

Solution: Not supported. Use column name and/or expression directly.  

### Casting a VARCHAR String to an INTEGER Causes an Error

Symptom: 

`SYSTEM ERROR: java.lang.NumberFormatException`  

Solution: Per the SQL specificationm CAST to INT does not support empty strings.  If you want to change this behavior, set the drill.exec.functions.cast_empty_string_to_null SESSION/SYSTEM option. 
 
### Unexpected Exception during Fragment Initialization

Symptom: An error occurred during the Foreman phase of the query. The error typically occurs due to the following common causes:  

* Malformed SQL that passed initial validation but failed upon further analysis
* Extraneous files in query directories do not match the default format type

Solution: Enable the verbose errors option and run the query again to see if further insight is provided.  

### Queries Running Out of Memory

Symptom: 

`RESOURCE ERROR: One or more nodes ran out of memory while executing the query.`  

Solution:  

* Increase the amount of direct memory allotted to Drill.
* If using CTAS, reduce the planner.width.max_per_node setting.
* Reduce the number of concurrent queries running on the cluster using Drill query queues.
* Disable hash aggregation and hash sort for your session.

See [Configuration Options]({{ site.baseurl }}/docs/configuration-options-introduction/).  

### Unclear Error Message

Symptom: Cannot determine issue from error message.

Solution: Turn on verbose errors. 

``ALTER SESSION SET `exec.errors.verbose`=true;``  

Determine your currently connected drillbit using `SELECT * FROM sys.drillbits.  Then review logs Drill logs from that drillbit.

### Error Starting Drill in Embedded Mode

Symptom:  

`java.net.BindException: Address already in use`  

Solution:  You can only run one Drillbit per node in embedded or distributed mode using default settings. You need to either change ports used by Drillbit or stop one Drillbit before starting another.
