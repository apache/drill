---
title: "Querying System Tables"
parent: "Query Data"
---
Drill has a sys database that contains system tables. You can query the system
tables for information about Drill, including Drill ports, the Drill version
running on the system, and available Drill options. View the databases in
Drill to identify the sys database, and then use the sys database to view
system tables that you can query.

## View Drill Databases

Issue the `SHOW DATABASES` command to view Drill databases.

    0: jdbc:drill:zk=10.10.100.113:5181> show databases;
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
    11 rows selected (0.162 seconds)

Drill returns `sys` in the database results.

## Use the Sys Database

Issue the `USE` command to select the sys database for subsequent SQL
requests.

    0: jdbc:drill:zk=10.10.100.113:5181> use sys;
    +------------+--------------------------------+
    |   ok     |  summary                         |
    +------------+--------------------------------+
    | true     | Default schema changed to 'sys'  |
    +------------+--------------------------------+
    1 row selected (0.101 seconds)

## View Tables

Issue the `SHOW TABLES` command to view the tables in the sys database.

    0: jdbc:drill:zk=10.10.100.113:5181> show tables;
    +--------------+------------+
    | TABLE_SCHEMA | TABLE_NAME |
    +--------------+------------+
    | sys          | drillbits  |
    | sys          | version    |
    | sys          | options    |
    +--------------+------------+
    3 rows selected (0.934 seconds)
    0: jdbc:drill:zk=10.10.100.113:5181>

## Query System Tables

Query the drillbits, version, and options tables in the sys database.

###Query the drillbits table.

    0: jdbc:drill:zk=10.10.100.113:5181> select * from drillbits;
    +-------------------+------------+--------------+------------+---------+
    |   host            |  user_port | control_port | data_port  |  current|
    +-------------------+------------+--------------+------------+--------+
    | qa-node115.qa.lab | 31010      | 31011        | 31012      | true    |
    | qa-node114.qa.lab | 31010      | 31011        | 31012      | false   |
    | qa-node116.qa.lab | 31010      | 31011        | 31012      | false   |
    +-------------------+------------+--------------+------------+---------+
    3 rows selected (0.146 seconds)

  * host   
The name of the node running the Drillbit service.
  * user-port  
The user port address, used between nodes in a cluster for connecting to
external clients and for the Drill Web UI.  
  * control_port  
The control port address, used between nodes for multi-node installation of
Apache Drill.
  * data_port  
The data port address, used between nodes for multi-node installation of
Apache Drill.
  * current  
True means the Drillbit is connected to the session or client running the
query. This Drillbit is the Foreman for the current session.  

### Query the version table.

    0: jdbc:drill:zk=10.10.100.113:5181> select * from version;
    +-------------------------------------------+--------------------------------------------------------------------+----------------------------+--------------+----------------------------+
    |                 commit_id                 |                           commit_message                           |        commit_time         | build_email  |         build_time         |
    +-------------------------------------------+--------------------------------------------------------------------+----------------------------+--------------+----------------------------+
    | d8b19759657698581cc0d01d7038797952888123  | DRILL-3100: TestImpersonationDisabledWithMiniDFS fails on Windows  | 15.05.2015 @ 05:18:03 UTC  | Unknown      | 15.05.2015 @ 06:52:32 UTC  |
    +-------------------------------------------+--------------------------------------------------------------------+----------------------------+--------------+----------------------------+
    1 row selected (0.099 seconds)
  * commit_id  
The github id of the release you are running. For example, <https://github.com
/apache/drill/commit/e3ab2c1760ad34bda80141e2c3108f7eda7c9104>
  * commit_message  
The message explaining the change.
  * commit_time  
The date and time of the change.
  * build_email  
The email address of the person who made the change, which is unknown in this
example.
  * build_time  
The time that the release was built.

### Query the options table.

Drill provides system, session, and boot options that you can query.

The following example shows a query on the system options:

    0: jdbc:drill:zk=10.10.100.113:5181> select * from options where type='SYSTEM' limit 10;
    +-------------------------------------------------+----------+---------+----------+-------------+-------------+-----------+------------+
    |                      name                       |   kind   |  type   |  status  |   num_val   | string_val  | bool_val  | float_val  |
    +-------------------------------------------------+----------+---------+----------+-------------+-------------+-----------+------------+
    | drill.exec.functions.cast_empty_string_to_null  | BOOLEAN  | SYSTEM  | DEFAULT  | null        | null        | false     | null       |
    | drill.exec.storage.file.partition.column.label  | STRING   | SYSTEM  | DEFAULT  | null        | dir         | null      | null       |
    | exec.errors.verbose                             | BOOLEAN  | SYSTEM  | DEFAULT  | null        | null        | false     | null       |
    | exec.java_compiler                              | STRING   | SYSTEM  | DEFAULT  | null        | DEFAULT     | null      | null       |
    | exec.java_compiler_debug                        | BOOLEAN  | SYSTEM  | DEFAULT  | null        | null        | true      | null       |
    | exec.java_compiler_janino_maxsize               | LONG     | SYSTEM  | DEFAULT  | 262144      | null        | null      | null       |
    | exec.max_hash_table_size                        | LONG     | SYSTEM  | DEFAULT  | 1073741824  | null        | null      | null       |
    | exec.min_hash_table_size                        | LONG     | SYSTEM  | DEFAULT  | 65536       | null        | null      | null       |
    | exec.queue.enable                               | BOOLEAN  | SYSTEM  | DEFAULT  | null        | null        | false     | null       |
    | exec.queue.large                                | LONG     | SYSTEM  | DEFAULT  | 10          | null        | null      | null       |
    +-------------------------------------------------+----------+---------+----------+-------------+-------------+-----------+------------+
    10 rows selected (0.216 seconds)

  * name  
The name of the option.
  * kind  
The data type of the option value.
  * type  
The type of options in the output: system, session, or boot.
  * num_val  
The default value, which is of the long or int data type; otherwise, null.
  * string_val  
The default value, which is a string; otherwise, null.
  * bool_val  
The default value, which is true or false; otherwise, null.
  * float_val  
The default value, which is of the double, float, or long double data type;
otherwise, null.

For information about how to configure Drill system and session options, see [Planning and Execution Options]({{ site.baseurl }}/docs/planning-and-execution-options).

For information about how to configure Drill start-up options, see [Start-Up Options]({{ site.baseurl }}/docs/start-up-options).

