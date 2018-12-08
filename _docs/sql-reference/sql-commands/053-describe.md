---
title: "DESCRIBE"
date: 2018-12-08
parent: "SQL Commands"
---
The DESCRIBE command returns information about columns in a table, view, or schema.

## Syntax

The DESCRIBE command supports the following syntax:  

       DESCRIBE [workspace.]table_name|view_name [column_name]
       DESCRIBE SCHEMA|DATABASE <name>[.workspace]
       DESCRIBE (query)



##Parameters  
*workspace*  
The location, within a schema, where a table or view exists.  
 
*view_name*  
The unique name of a view.  

*table_name*  
The unique name of a table.  

*column_name*  
Optional list of column names in a table or view.  

*schema/database*  
A configured storage plugin instance with or without a configured workspace.  

*query*  
A SELECT statement that defines the columns and rows in the table or view.  

*expression*  
An expression formed from one or more columns that exist in the tables or views referenced by the query. 
 


## Usage Notes

Drill only supports SQL data types. Verify that all data types in an external data source, such as Hive or HBase, map to supported data types in Drill. See [Data Types]({{site.baseurl}}/docs/data-types/) for more information.  

###DESCRIBE
- You can issue the DESCRIBE command against views created in a workspace, tables created in Hive and HBase, or schemas.  
- You can issue the DESCRIBE command on a table or view from any schema. For example, if you are working in the dfs.myworkspace schema, you can issue the DESCRIBE command on a view or table in another schema, such hive or dfs.devworkspace.  
- Currently, DESCRIBE does not support tables created in a file system.

###DESCRIBE SCHEMA  
- You can issue the DESCRIBE SCHEMA command on any schema. However, you can only include workspaces for file schemas, such as `dfs.myworkspace`.  
- When you issue the DESCRIBE SCHEMA command on a particular schema, Drill returns all of the schema properties. The schema properties correlate with the configuration information in the Storage tab of the Drill Web UI for that schema.  
- When you issue DESCRIBE SCHEMA against a schema and workspace, such as `dfs.myworkspace`, Drill returns the workspace properties in addition to all of the schema properties.  
- When you issue DESCRIBE SCHEMA against the `dfs` schema, Drill also returns the properties of the “default” workspace. Issuing DESCRIBE SCHEMA against `dfs` or `` dfs.`default` `` returns the same results. 


## Examples

###DESCRIBE  

The following example demonstrates the steps that you can follow when you want
to use the DESCRIBE command to see column information for a view and for Hive
and HBase tables.

Complete the following steps to use the DESCRIBE command:

  1. Issue the USE command to switch to a particular schema.

        0: jdbc:drill:zk=drilldemo:5181> use hive;
        +------------+------------+
        |   ok  |  summary   |
        +------------+------------+
        | true      | Default schema changed to 'hive' |
        +------------+------------+
        1 row selected (0.025 seconds)

  2. Issue the SHOW TABLES command to see the existing tables in the schema.

        0: jdbc:drill:zk=drilldemo:5181> show tables;
        +--------------+------------+
        | TABLE_SCHEMA | TABLE_NAME |
        +--------------+------------+
        | hive.default | orders     |
        | hive.default | products   |
        +--------------+------------+
        2 rows selected (0.438 seconds)

  3. Issue the DESCRIBE command on a table.

        0: jdbc:drill:zk=drilldemo:5181> describe orders;
        +-------------+------------+-------------+
        | COLUMN_NAME | DATA_TYPE  | IS_NULLABLE |
        +-------------+------------+-------------+
        | order_id  | BIGINT    | YES       |
        | month     | VARCHAR   | YES       |
        | purchdate   | TIMESTAMP  | YES        |
        | cust_id   | BIGINT    | YES       |
        | state     | VARCHAR   | YES       |
        | prod_id   | BIGINT    | YES       |
        | order_total | INTEGER | YES       |
        +-------------+------------+-------------+
        7 rows selected (0.64 seconds)

  4. Issue the DESCRIBE command on a table in another schema from the current schema.

        0: jdbc:drill:zk=drilldemo:5181> describe hbase.customers;
        +-------------+------------+-------------+
        | COLUMN_NAME | DATA_TYPE  | IS_NULLABLE |
        +-------------+------------+-------------+
        | row_key   | ANY       | NO        |
        | address   | (VARCHAR(1), ANY) MAP | NO        |
        | loyalty   | (VARCHAR(1), ANY) MAP | NO        |
        | personal  | (VARCHAR(1), ANY) MAP | NO        |
        +-------------+------------+-------------+
        4 rows selected (0.671 seconds)

  5. Issue the DESCRIBE command on a view in another schema from the current schema.

        0: jdbc:drill:zk=drilldemo:5181> describe dfs.views.customers_vw;
        +-------------+------------+-------------+
        | COLUMN_NAME | DATA_TYPE  | IS_NULLABLE |
        +-------------+------------+-------------+
        | cust_id   | BIGINT    | NO        |
        | name      | VARCHAR   | NO        |
        | address   | VARCHAR   | NO        |
        | gender    | VARCHAR   | NO        |
        | age       | VARCHAR   | NO        |
        | agg_rev   | VARCHAR   | NO        |
        | membership  | VARCHAR | NO        |
        +-------------+------------+-------------+
        7 rows selected (0.403 seconds)  
 
6. Issue the DESCRIBE SCHEMA command on `dfs.tmp` (the `dfs` schema and `tmp` workspace configured within the `dfs` schema) from the current schema.  
  
        0: jdbc:drill:zk=drilldemo:5181> describe schema dfs.tmp;  
       
              {
                "type" : "file",
                "enabled" : true,
                "connection" : "file:///",
                "config" : null,
                "formats" : {
                  "psv" : {
                    "type" : "text",
                    "extensions" : [ "tbl" ],
                    "delimiter" : "|"
                  },
                  "csv" : {
                    "type" : "text",
                    "extensions" : [ "csv", "bcp" ],
                    "delimiter" : ","
                  },
                 ... 
                },
                "location" : "/tmp",
                "writable" : true,
                "defaultInputFormat" : null
              }  



       
              
       
