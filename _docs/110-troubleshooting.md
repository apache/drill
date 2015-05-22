---
title: "Troubleshooting"
---

You may experience certain known issues when using Drill. This document lists some known issues and resolutions for each.

## Before You Begin
Before you begin troubleshooting issues that you encounter in Drill, make sure you know which Drillbit is acting as the Foreman in the Drill cluster. The Drill node to which a client or application connects is the Foreman. 

You should also know the version of Drill running in the cluster. You can search JIRA for issues related to the version to see if a patch or workaround exists for the issue that you encountered.

### Identify the Foreman
Issue the following query to identify the node running as the Foreman:  

       SELECT host FROM sys.drillbits WHERE `current` = true;

### Identify the Drill Version
Issue the following query to identify the version of Drill running in your cluster:
SELECT commit_id FROM sys.version;

### Enable Verbose Errors
You can enable the verbose errors option for a more detailed error print-out.

Issue the following command to enable the verbose errors option:  

       ALTER SESSION SET `exec.errors.verbose` = true

## Troubleshooting
If you have any issues in Drill, search the following list for your issue and apply the suggested solution:

**Query Parsing Errors**  
Symptom:  

       PARSE ERROR: At line x, column x: ...
Solution: Verify that you are using valid syntax. See [SQL Reference]({{ site.baseurl }}/docs/sql-reference-introduction/).
If you are using common words, they may be reserved words.  Make sure to use back ticks
Confirm that you are using back ticks to quote identifiers when using special characters such as back slashes or periods from a file path.

**Reserved Words**  
Symptom:   

       select count from dfs.drill.`test2.json`;
       Query failed: SYSTEM ERROR: Failure parsing SQL. Encountered "count from" at line 1, column 8.
       Was expecting one of:
           "UNION" ...
           "INTERSECT" ...
Solution: Fix with correct syntax. See [Reserved Keywords]({{ site.baseurl }}/docs/reserved-keywords/).

       select `count` from dfs.drill.`test2.json`;  

**Tables not found**  
Symptom:
 
       select * from dfs.drill.test2.json;
       Query failed: PARSE ERROR: From line 1, column 15 to line 1, column 17: Table 'dfs.drill.test2.json' not found  

Solutions:

1. Run SHOW FILES to list the files in the dfs.drill workspace. 
2. Check the permission of the files with those for the the Drill user.  
3. Verify backticks added for file name: select * from dfs.drill.``test2.json``;  
4. Drill may not be able to determine the type of file you are trying to read. Try using Drill Default Input Format.  
5. Verify that your storage plugin is correctly configured.
6. Verify that Drill can auto-detect your file format.  Drill supports auto-detection for the following formats:  
 * CSV
 * TSV
 * PSV
 * Parquet
 * JSON

**Access nested fields without table name/alias**  
Symptom: 

       select x.y …  
       PARSE ERROR: At line 1, column 8: Table 'x' not found  
Solution: Add table name or alias to the field reference:  

       select t.x.y from t  

**Unexpected null values for columns in results**  
Symptom:  The following type of query returns NULL values:  

       select t.price from t 


Solution: Drill is schema-less system. Verify that column names are typed correctly.


**Using functions with incorrect data types**  

Symptom: Example  

       select trunc(c3) from t3;
       
       0: jdbc:drill:schema=dfs> select trunc(c3) from t3;
       Query failed: SYSTEM ERROR: Failure while trying to materialize incoming schema.  Errors:
        
       Error in expression at index -1.  Error: Missing function implementation: [trunc(DATE-OPTIONAL)].  Full expression: --UNKNOWN EXPRESSION--..
       
       Fragment 0:0
       
       [6e465594-4d83-4042-b88d-50e7eb207484 on atsqa4-133.qa.lab:31010]
       Error: exception while executing query: Failure while executing query. (state=,code=0)  

Solution: Ensure that the function is invoked with the correct data type parameters. In the example above, c3 is an unsupported date type. 

**Query takes a long time to return** 

Symptom: Query takes longer to return than expected.

Solution: Review the [query profile]({{ site.baseurl }}/docs/query-profiles/) and:  

 * Determine whether progress is being made (look at last update and last change times).
 * Look at where Drill is currently spending time and try to optimize those operations.
 * Confirm that Drill is taking advantage of the nature of your data, including things like partition pruning and projection pushdown.

**Schema changes**  

Symptom:  

       DATA_READ ERROR: Error parsing JSON - You tried to write a XXXX type when you are using a ValueWriter of type XXXX.       
       File  /src/data/schema.json
       Record  2
       Fragment 0:0  

Solution: Drill does not fully support schema changes.  In this case, you will need to either ensure that your schemas are the same or only select columns that share schema.

**Timestamps and Timezones other than UTC**  

Symptoms: Issues with timestamp and timezone. Illegal instant due to time zone offset transition (America/New_York)

Solution: Convert data to UTC format. You are most likely trying to import date and time data that is encoded one timezone in a different timezone.  Drill’s default behavior is to use the system’s time for converting incoming data.  If you are providing UTC data and your Drillbit nodes do not run with UTC time, you’ll need to run your JVM with the following system property:

     -Duser.timezone=UTC  

 `http://www.openkb.info/2015/05/understanding-drills-timestamp-and.html `  

**Unexpected ODBC issues**  

Symptom: ODBC errors.

Solution: Make sure that the ODBC driver version is compatible with the server version. 
Turn on ODBC driver debug logging to better understand failure.  

**Connectivity issues when connecting via ZooKeeper for JDBC/ODBC**  

Symptom: Client cannot resolve ZooKeeper host names for JDBC/ODBC.

Solution: Ensure that Zookeeper is up and running. Verify that Drill has the correct drill-override.conf settings for the Zookeeper quorum.

**Metadata queries take a long time to return**  

Symptom: Running SHOW databases/schemas/tables hangs (in general any information_schema queries hang).

Solution: Disable incorrectly configured storage plugins or start appropriate services. Check compatibility matrix for the appropriate versions.  

**Unexpected results due to implicit casting**  

Symptom: rill implicitly casts based on order of precedence.

Solution: Review Drill casting behaviors and explicitly cast for the expected results. See [Data Types]({{ site.baseurl }}/docs/handling-different-data-types/).

**Column alias causes an error**  

Symptom: Drill is not case sensitive, and you can provide any alias for a column name. However, if the storage type is case sensitive, the alias name may conflict and cause errors.

Solution: Verify that the column alias does not conflict with the storage type. See [Lexical Structures]({{ site.baseurl }}/docs/lexical-structure/#case-sensitivity).  

**List (arrays) contains null**  

Symptom: UNSUPPORTED\_OPERATION ERROR: Null values are not supported in lists by default. Please set store.json.all\_text_mode to true to read lists containing nulls. Be advised that this will treat JSON null values as a string containing the word 'null'.

Solution: Change Drill session settings to enable all_text_mode per message.  
Avoid selecting fields that are arrays containing nulls.

**SELECT COUNT (\*) takes a long time to run**  

Solution: In come cases, the underlying storage format does not have a built-in capability to return a count of records in a table.  In these cases, Drill will do a full scan of the data to verify the number of records.

**Tableau issues**  

Symptom: You see a lot of error messages in ODBC trace files or the performance is slow.

Solution: Verify that you have installed the TDC file shipped with the ODBC driver.  

**Group by using alias**  

Symptom: Invalid column.

Solution: Not supported. Use column name and/or expression directly.  

**Casting a Varchar string to an integer results in an error**  

Symptom: 

       SYSTEM ERROR: java.lang.NumberFormatException

Solution: Per the ANSI SQL specification CAST to INT does not support empty strings.  If you want to change this behavior, you can set Drill to use the cast empty string to null behavior.  This can be done using the drill.exec.functions.cast_empty_string_to_null SESSION/SYSTEM option. 
 
**Unexpected exception during fragment initialization**  

Symptom: The error occurred during the Foreman phase of the query. The error typically occurs due to the following common causes:  

* Malformed SQL that passed initial validation but failed upon further analysis
* Extraneous files in query directories do not match the default format type

Solution: Enable the verbose errors option and run the query again to see if further insight is provided.  

**Queries running out of memory**  

Symptom: 

       RESOURCE ERROR: One or more nodes ran out of memory while executing the query.

Solution:  

* Increase the amount of direct memory allotted to Drill
* If using CTAS, reduce the planner.width.max_per_node setting
* Reduce the number of concurrent queries running on the cluster using Drill query queues
* Disable hash aggregation and hash sort for your session
* See [Configuration Options]({{ site.baseurl }}/docs/configuration-options-introduction/)  

**Unclear Error Message**  

Symptom: Cannot determine issue from error message.

Solution: Turn on verbose errors. 

       alter session set `exec.errors.verbose`=true;

Determine your currently connected drillbit using select * from sys.drillbits.  Then review logs Drill logs from that drillbit.

**SQLLine error starting Drill in embedded mode**  

Symptom:  

       java.net.BindException: Address already in use  

Solution:  You can only run one Drillbit per node(embedded or daemon) using default settings.  You need to either change ports used by Drillbit or stop one Drillbit before starting another.
 





























       







