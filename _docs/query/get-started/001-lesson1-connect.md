---
title: "Lesson 1: Connect to Data Sources"
parent: "Getting Started Tutorial"
---
This lesson shows how to connect to default data sources that Drill installs
and configures through storage plugins. You learn how to list the storage
plugins as you would list databases in SQL.

## List the Storage Plugins

To list the default storage plugins, use the SHOW DATABASES command.

  1. Issue the SHOW DATABASES command.
    
        0: jdbc:drill:zk=local> SHOW DATABASES;  
     The output lists the storage plugins, which you use as a SQL database, in
<database>.<workspace> format.

        +-------------+
        | SCHEMA_NAME |
        +-------------+
        | dfs.default |
        | dfs.root    |
        | dfs.tmp     |
        | cp.default  |
        | sys         |
        | INFORMATION_SCHEMA |
        +-------------+
        6 rows selected (0.977 seconds)

  2. Take a look at the list of storage plugins and workspaces that Drill recognizes.

* `dfs` is the storage plugin for connecting to the [file system](/drill/docs/querying-a-file-system) data source on your machine.
* `cp` is a storage plugin for connecting to a JAR data source used with MapR.
* `sys` is a storage plugin for connecting to Drill [system tables](/drill/docs/querying-system-tables).
* [INFORMATION_SCHEMA](/drill/docs/querying-the-information-schema) is a storage plugin for connecting to an ANSI standard set of metadata tables.

## List Tables

You choose a storage plugin using the USE command. The output shows the status
and description of the operation. After connecting to a data source, you can
list available tables.

  1. Select the `sys` storage plugin.
  
          USE sys;
          +------------+------------+
          |     ok     |  summary   |
          +------------+------------+
          | true       | Default schema changed to 'sys' |
          +------------+------------+
          1 row selected (0.034 seconds) 
  2. List the tables in `sys`.
  
          SHOW TABLES;
          0: jdbc:drill:zk=local> SHOW TABLES;  
          
          +--------------+------------+
          | TABLE_SCHEMA | TABLE_NAME |
          +--------------+------------+
          | sys          | drillbits  |
          | sys          | version    |
          | sys          | options    |
          +--------------+------------+
  3. Select the INFORMATION_SCHEMA storage plugin.
  
          0: jdbc:drill:zk=local> USE INFORMATION_SCHEMA;
 
          +------------+------------+
          |     ok     |  summary   |
          +------------+------------+
          | true       | Default schema changed to 'INFORMATION_SCHEMA' |
          +------------+------------+
          1 row selected (0.023 seconds)
  4. List the tables in INFORMATION_SCHEMA.

          0: jdbc:drill:zk=local> SHOW TABLES;  
 
          +--------------+------------+
          | TABLE_SCHEMA | TABLE_NAME |
          +--------------+------------+
          | INFORMATION_SCHEMA | VIEWS      |
          | INFORMATION_SCHEMA | COLUMNS    |
          | INFORMATION_SCHEMA | TABLES     |
          | INFORMATION_SCHEMA | CATALOGS   |
          | INFORMATION_SCHEMA | SCHEMATA   |
          +--------------+------------+
          5 rows selected (0.082 seconds)
