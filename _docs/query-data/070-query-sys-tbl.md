---
title: "Querying System Tables"
date: 2018-12-10
parent: "Query Data"
---  

Drill has a sys database that contains system tables. You can query the system tables for information about Drill, such as available configuration options and the Drill version running on the system. Starting in Drill 1.15, you can query the functions table to expose built-in SQL functions and user-defined functions. 

## Viewing Drill Databases

When you run the `SHOW DATABASES` command, Drill returns the list of available databases that you can query. In the following example, you can see that Drill returns the sys database:  

    SHOW DATABASES;
    +--------------------+
    |      SCHEMA_NAME   |
    +--------------------+
    | M7                 |
    | hive.default       |
    | dfs.default        |
    | dfs.root           |
    | dfs.views          |
    | dfs.tmp            |
    | dfs.tpcds          |
    | sys                |
    | cp.default         |
    | hbase              |
    | INFORMATION_SCHEMA |
    +--------------------+  


## Switching to the sys Database 

The USE command changes the schema context to the specified schema. When you switch to the sys schema, as shown, Drill issues all subsequent queries against that schema only:  
 

   	USE sys;
    +-------+----------------------------------+
    |  ok   |             summary              |
    +-------+----------------------------------+
    | true  | Default schema changed to [sys]  |
    +-------+----------------------------------+
    1 row selected (0.101 seconds)

## Viewing System Tables

When you run the `SHOW TABLES` command against the sys database, Drill returns the list of system tables that you can query for information about Drill:

	SHOW TABLES;
	+---------------+-----------------------+
	| TABLE_SCHEMA  |      TABLE_NAME       |
	+---------------+-----------------------+
	| sys           | memory                |
	| sys           | functions             |
	| sys           | internal_options_old  |
	| sys           | profiles              |
	| sys           | internal_options      |
	| sys           | threads               |
	| sys           | version               |
	| sys           | options_old           |
	| sys           | profiles_json         |
	| sys           | options               |
	| sys           | drillbits             |
	| sys           | boot                  |
	| sys           | connections           |
	+---------------+-----------------------+


## Querying System Tables

The following sections show examples of queries against the system tables available in the sys database.

###Querying the drillbits Table

    SELECT * FROM drillbits;
    +-------------------+------------+--------------+------------+---------+
    |   hostname        |  user_port | control_port | data_port  |  current|
    +-------------------+------------+--------------+------------+--------+
    | qa-node115.qa.lab | 31010      | 31011        | 31012      | true    |
    | qa-node114.qa.lab | 31010      | 31011        | 31012      | false   |
    | qa-node116.qa.lab | 31010      | 31011        | 31012      | false   |
    +-------------------+------------+--------------+------------+---------+
    

  * **hostname**   
The name of the node running the Drillbit service.
  * **user_port**  
The user port address, used between nodes in a cluster for connecting to
external clients and for the Drill Web UI.  
  * **control_port**  
The control port address, used between nodes for multi-node installation of
Apache Drill.
  * **data_port**  
The data port address, used between nodes for multi-node installation of
Apache Drill.
  * **current**  
True means the Drillbit is connected to the session or client running the
query. This Drillbit is the Foreman for the current session.  

### Querying the version Table

    SELECT * FROM version;
    +-------------------------------------------+--------------------------------------------------------------------+----------------------------+--------------+----------------------------+
    |                 commit_id                 |                           commit_message                           |        commit_time         | build_email  |         build_time         |
    +-------------------------------------------+--------------------------------------------------------------------+----------------------------+--------------+----------------------------+
    | d8b19759657698581cc0d01d7038797952888123  | DRILL-3100: TestImpersonationDisabledWithMiniDFS fails on Windows  | 15.05.2015 @ 05:18:03 UTC  | Unknown      | 15.05.2015 @ 06:52:32 UTC  |
    +-------------------------------------------+--------------------------------------------------------------------+----------------------------+--------------+----------------------------+

  * **commit_id**  
The github id of the release you are running. For example, `https://github.com
/apache/drill/commit/e3ab2c1760ad34bda80141e2c3108f7eda7c9104`
  * **commit_message**  
The message explaining the change.
  * **commit_time**  
The date and time of the change.
  * **build_email**  
The email address of the person who made the change, which is unknown in this
example.
  * **build_time**  
The time that the release was built.


### Querying the boot Table

    SELECT * FROM boot LIMIT 10;
    +--------------------------------------+----------+-------+---------+------------+-------------------------+-----------+------------+
    |                 name                 |   kind   | type  | status  |  num_val   |       string_val        | bool_val  | float_val  |
    +--------------------------------------+----------+-------+---------+------------+-------------------------+-----------+------------+
    | awt.toolkit                          | STRING   | BOOT  | BOOT    | null       | "sun.awt.X11.XToolkit"  | null      | null       |
    | drill.client.supports-complex-types  | BOOLEAN  | BOOT  | BOOT    | null       | null                    | true      | null       |
    | drill.exec.buffer.size               | STRING   | BOOT  | BOOT    | null       | "6"                     | null      | null       |
    | drill.exec.buffer.spooling.delete    | BOOLEAN  | BOOT  | BOOT    | null       | null                    | true      | null       |
    | drill.exec.buffer.spooling.size      | LONG     | BOOT  | BOOT    | 100000000  | null                    | null      | null       |
    | drill.exec.cluster-id                | STRING   | BOOT  | BOOT    | null       | "SKCluster"             | null      | null       |
    | drill.exec.compile.cache_max_size    | LONG     | BOOT  | BOOT    | 1000       | null                    | null      | null       |
    | drill.exec.compile.compiler          | STRING   | BOOT  | BOOT    | null       | "DEFAULT"               | null      | null       |
    | drill.exec.compile.debug             | BOOLEAN  | BOOT  | BOOT    | null       | null                    | true      | null       |
    | drill.exec.compile.janino_maxsize    | LONG     | BOOT  | BOOT    | 262144     | null                    | null      | null       |
    +--------------------------------------+----------+-------+---------+------------+-------------------------+-----------+------------+
    10 rows selected (0.192 seconds)

  * **name**  
The name of the boot option.
  * **kind**  
The data type of the option value.
  * **type**  
This is always boot.
  * **status**
This is always boot.
  * **num_val**  
The default value, which is of the long or int data type; otherwise, null.
  * **string_val**  
The default value, which is a string; otherwise, null.
  * **bool_val**  
The default value, which is true or false; otherwise, null.
  * **float_val**  
The default value, which is of the double, float, or long double data type;
otherwise, null.

### Querying the threads Table

    SELECT * FROM threads;
    +--------------------+------------+----------------+---------------+
    |       hostname     | user_port  | total_threads  | busy_threads  |
    +--------------------+------------+----------------+---------------+
    | qa-node115.qa.lab  | 31010      | 33             | 33            |
    | qa-node114.qa.lab  | 31010      | 33             | 32            |
    | qa-node116.qa.lab  | 31010      | 29             | 29            |
    +--------------------+------------+----------------+---------------+
    3 rows selected (0.618 seconds)

  * **hostname**   
The name of the node running the Drillbit service.
  * **user_port**  
The user port address, used between nodes in a cluster for connecting to
external clients and for the Drill Web UI. 
  * **total_threads**
The peak thread count on the node.
  * **busy_threads**
The current number of live threads (daemon and non-daemon) on the node.

### Querying the memory Table

    SELECT * FROM memory;
    +--------------------+------------+---------------+-------------+-----------------+---------------------+-------------+
    |       hostname     | user_port  | heap_current  |  heap_max   | direct_current  | jvm_direct_current  | direct_max  |
    +--------------------+------------+---------------+-------------+-----------------+---------------------+-------------+
    | qa-node115.qa.lab  | 31010      | 443549712     | 4294967296  | 11798941        | 167772974           | 8589934592  |
    | qa-node114.qa.lab  | 31010      | 149948432     | 4294967296  | 7750365         | 134218542           | 8589934592  |
    | qa-node116.qa.lab  | 31010      | 358612992     | 4294967296  | 7750365         | 83886894            | 8589934592  |
    +--------------------+------------+---------------+-------------+-----------------+---------------------+-------------+  
  

  * **hostname**   
The name of the node running the Drillbit service.
  * **user_port**  
The user port address, used between nodes in a cluster for connecting to
external clients and for the Drill Web UI.
  * **heap_current**
The amount of memory being used on the heap, in bytes.
  * **heap_max**
The maximum amount of memory available on the heap, in bytes.
  * **direct_current**
The current direct memory being used by the allocator, in bytes.
  * **jvm_direct_current**
The current JVM direct memory allocation, in bytes.
  * **direct_max**
The maximum direct memory available to the allocator, in bytes.  


### Querying the connections Table  

	SELECT * FROM connections limit 3;
	+------------+--------------+------------+--------------------------+------------------------+----------+------------------+--------------+-----------+---------------------------------------+
	|    user    |    client    |  drillbit  |       established        |        duration        | queries  | isAuthenticated  | isEncrypted  | usingSSL  |                session                |
	+------------+--------------+------------+--------------------------+------------------------+----------+------------------+--------------+-----------+---------------------------------------+
	| anonymous  | 10.10.10.10  | test.lab  | 2018-12-07 15:40:50.857  | 1 hr 32 min 7.670 sec  | 20       | false            | false        | false     | 484c7c8c-2ecd-4f1e-b42d-e3c8750b4ce5  |
	+------------+--------------+------------+--------------------------+------------------------+----------+------------------+--------------+-----------+---------------------------------------+  

The connections table provides the following information about the connection to the Drillbits, including:  


- name of the user connecting to the Drillbit from the client
- ip address of the client
- name of the Drillbit to which the client has connected
- time at which the connection was established
- duration of the connection 
- number of queries run during the connection
- security for the connection
- session ID   

### Querying the profiles_json Table 

		SELECT * FROM profiles_json LIMIT 1;
		+---------------------------------------+----------------------------------------------------------------------------------+
		|                queryId                |                                       json                                       |
		+---------------------------------------+----------------------------------------------------------------------------------+
		| 23f4e2a7-5d60-a766-0b03-d7a03e99033e  | {"id":{"part1":2590944894098909030,"part2":793715042592293694},"type":1,"start":1544232280280,"end":1544232280309,"query":"SELECT * FROM profiles_json LIMIT 0","plan":"00-00    Screen : rowType = RecordType(VARCHAR(65536) queryId, VARCHAR(65536) json): rowcount = 1.0, cumulative cost = {1.1 rows, 2.1 cpu, 1.0 io, 0.0 network, 0.0 memory}, id = 2115\n00-01      Project(queryId=[$0], json=[$1]) : rowType = RecordType(VARCHAR(65536) queryId, VARCHAR(65536) json): rowcount = 1.0, cumulative cost = {1.0 rows, 2.0 cpu, 1.0 io, 0.0 network, 0.0 memory}, id = 2114\n00-02        DirectScan(groupscan=[RelDataTypeReader{columnNames=[queryId, json], columnTypes=[VARCHAR-REQUIRED, VARCHAR-OPTIONAL]}]) : rowType = RecordType(VARCHAR(65536) queryId, VARCHAR(65536) json): rowcount = 1.0, cumulative cost = {0.0 rows, 0.0 cpu, 1.0 io, 0.0 network, 0.0 memory}, id = 2113\n","foreman":{"address":"doc23.lab","userPort":31010,"controlPort":31011,"dataPort":31012,"version":"1.15.0-SNAPSHOT","state":0},"state":2,"totalFragments":1,"finishedFragments":0,"fragmentProfile":[{"majorFragmentId":0,"minorFragmentProfile":[{"state":3,"minorFragmentId":0,"operatorProfile":[{"inputProfile":[{"records":0,"batches":1,"schemas":1}],"operatorId":2,"operatorType":26,"setupNanos":0,"processNanos":780670,"peakLocalMemoryAllocated":102400,"waitNanos":0},{"inputProfile":[{"records":0,"batches":1,"schemas":1}],"operatorId":1,"operatorType":10,"setupNanos":1600465,"processNanos":207117,"peakLocalMemoryAllocated":102400,"waitNanos":0},{"inputProfile":[{"records":0,"batches":1,"schemas":1}],"operatorId":0,"operatorType":13,"setupNanos":0,"processNanos":229998,"peakLocalMemoryAllocated":0,"metric":[{"metricId":0,"longValue":0}],"waitNanos":1246096}],"startTime":1544232280300,"endTime":1544232280307,"memoryUsed":0,"maxMemoryUsed":3000000,"endpoint":{"address":"doc23.lab","userPort":31010,"controlPort":31011,"dataPort":31012,"version":"1.15.0-SNAPSHOT","state":0},"lastUpdate":1544232280307,"lastProgress":1544232280307}]}],"user":"anonymous","optionsJson":"[ {\n  \"kind\" : \"BOOLEAN\",\n  \"accessibleScopes\" : \"ALL\",\n  \"name\" : \"exec.return_result_set_for_ddl\",\n  \"bool_val\" : true,\n  \"scope\" : \"QUERY\"\n} ]","planEnd":1544232280300,"queueWaitEnd":1544232280300,"totalCost":2.0,"queueName":"Unknown","queryId":"23f4e2a7-5d60-a766-0b03-d7a03e99033e"} |
		+---------------------------------------+----------------------------------------------------------------------------------+  

The profiles_json table provides the query profile in JSON format for all queries, by queryID.  

### Querying the options Table  

The options table contains system, session, and boot configuration options available in Drill. Starting in Drill 1.15, a new options table lists option descriptions. You can query the latest and previous options table, as shown in the examples below.  

**Note:** Option names are case-sensitive.

The following query selects all of the options related to memory from the new options table:
    
  
 	SELECT * FROM options WHERE name LIKE '%memory%'; 
	+----------------------------------------------------------------+---------+-------------------+-------------+----------+--------------+----------------------------------------------------------------------------------+
	|                              name                              |  kind   | accessibleScopes  |     val     |  status  | optionScope  |                                   description                                    |
	+----------------------------------------------------------------+---------+-------------------+-------------+----------+--------------+----------------------------------------------------------------------------------+
	| drill.exec.memory.operator.output_batch_size                   | BIGINT  | SYSTEM            | 16777216    | DEFAULT  | BOOT         | Available as of Drill 1.13. Limits the amount of memory that the Flatten, Merge Join, and External Sort ope... |
	| drill.exec.memory.operator.output_batch_size_avail_mem_factor  | FLOAT   | SYSTEM            | 0.1         | DEFAULT  | BOOT         | Based on the available system memory, adjusts the output batch size for buffered operators by the factor set. |
	| exec.hashagg.use_memory_prediction                             | BIT     | ALL               | true        | DEFAULT  | BOOT         | Enables Hash Aggregates to use memory predictions to proactively spill early. Default is true. |
	| exec.queue.memory_ratio                                        | FLOAT   | SYSTEM            | 10.0        | DEFAULT  | BOOT         |                                                                                  |
	| exec.queue.memory_reserve_ratio                                | FLOAT   | SYSTEM            | 0.2         | DEFAULT  | BOOT         |                                                                                  |
	| planner.memory.average_field_width                             | BIGINT  | ALL               | 8           | DEFAULT  | BOOT         | Used in estimating memory requirements.                                          |
	| planner.memory.enable_memory_estimation                        | BIT     | ALL               | false       | DEFAULT  | BOOT         | Toggles the state of memory estimation and re-planning of the query. When enabled, Drill conservatively est... |
	| planner.memory.hash_agg_table_factor                           | FLOAT   | ALL               | 1.1         | DEFAULT  | BOOT         | A heuristic value for influencing the size of the hash aggregation table.        |
	| planner.memory.hash_join_table_factor                          | FLOAT   | ALL               | 1.1         | DEFAULT  | BOOT         | A heuristic value for influencing the size of the hash aggregation table.        |
	| planner.memory.max_query_memory_per_node                       | BIGINT  | ALL               | 2147483750  | CHANGED  | SYSTEM       | Sets the maximum amount of direct memory allocated to the Sort and Hash Aggregate operators during each que... |
	| planner.memory.min_memory_per_buffered_op                      | BIGINT  | ALL               | 41943040    | DEFAULT  | BOOT         | Minimum memory allocated to each buffered operator instance                      |
	| planner.memory.non_blocking_operators_memory                   | BIGINT  | ALL               | 64          | DEFAULT  | BOOT         | Extra query memory per node for non-blocking operators. This option is currently used only for memory estim... |
	| planner.memory.percent_per_query                               | FLOAT   | ALL               | 0.05        | DEFAULT  | BOOT         | Sets the memory as a percentage of the total direct memory.                      |
	| planner.memory_limit                                           | BIGINT  | ALL               | 268435456   | DEFAULT  | BOOT         | Defines the maximum amount of direct memory allocated to a query for planning. When multiple queries run co... |
	+----------------------------------------------------------------+---------+-------------------+-------------+----------+--------------+----------------------------------------------------------------------------------+  

	
The following query selects three of the options related to memory from the pre-1.15 options table:  

**Note:** Notice that the table contains _val columns, which have been removed from the latest options table (introduced in Drill 1.15).

	SELECT * FROM options_old WHERE name LIKE '%memory%' LIMIT 3;
	+----------------------------------------------------------------+----------+-------------------+--------------+----------+-----------+-------------+-----------+------------+
	|                              name                              |   kind   | accessibleScopes  | optionScope  |  status  |  num_val  | string_val  | bool_val  | float_val  |
	+----------------------------------------------------------------+----------+-------------------+--------------+----------+-----------+-------------+-----------+------------+
	| drill.exec.memory.operator.output_batch_size                   | LONG     | SYSTEM            | BOOT         | DEFAULT  | 16777216  | null        | null      | null       |
	| drill.exec.memory.operator.output_batch_size_avail_mem_factor  | DOUBLE   | SYSTEM            | BOOT         | DEFAULT  | null      | null        | null      | 0.1        |
	| exec.hashagg.use_memory_prediction                             | BOOLEAN  | ALL               | BOOT         | DEFAULT  | null      | null        | true      | null       |
	+----------------------------------------------------------------+----------+-------------------+--------------+----------+-----------+-------------+-----------+------------+


### Querying the functions Table 

The functions table is available starting in Drill 1.15. The functions table exposes the available SQL functions in Drill and also detects UDFs that have been dynamically loaded into Drill. You can query the functions table to see the following information about built-in and user-defined functions in Drill:

	SELECT * FROM functions LIMIT 0;
	+-------+------------+-------------+---------+-----------+
	| name  | signature  | returnType  | source  | internal  |
	+-------+------------+-------------+---------+-----------+
	+-------+------------+-------------+---------+-----------+


The following query shows the count of built-in functions and UDFs:  
**Note:** In this example, UDFs were dynamically loaded into Drill, as indicated by the JAR files.

	SELECT source, COUNT(*) AS functionCount FROM sys.functions GROUP BY source;
	+-----------------------------------------+----------------+
	|                 source                  | functionCount  |
	+-----------------------------------------+----------------+
	| built-in                                | 2704           |
	| simple-drill-function-1.0-SNAPSHOT.jar  | 12             |
	| drill-url-tools-1.0.jar                 | 1              |
	+-----------------------------------------+----------------+   

You can search the functions table to see if Drill supports specific functions. For example, the following query shows all the functions with names that include sql:   

	SELECT * FROM functions WHERE name LIKE '%sql%';
	+-------------------+------------------------------------+-------------+-----------+-----------+
	|       name        |             signature              | returnType  |  source   | internal  |
	+-------------------+------------------------------------+-------------+-----------+-----------+
	| sqlTypeOf         | LATE-REQUIRED                      | VARCHAR     | built-in  | false     |
	| sql_to_date       | VARCHAR-REQUIRED,VARCHAR-REQUIRED  | DATE        | built-in  | false     |
	| sql_to_time       | VARCHAR-REQUIRED,VARCHAR-REQUIRED  | TIME        | built-in  | false     |
	| sql_to_timestamp  | VARCHAR-REQUIRED,VARCHAR-REQUIRED  | TIMESTAMP   | built-in  | false     |
	+-------------------+------------------------------------+-------------+-----------+-----------+  	


For information about how to configure Drill system and session options, see [Planning and Execution Options]({{ site.baseurl }}/docs/planning-and-execution-options).

For information about how to configure Drill start-up options, see [Start-Up Options]({{ site.baseurl }}/docs/start-up-options).

