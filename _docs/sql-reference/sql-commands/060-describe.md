---
title: "DESCRIBE"
parent: "SQL Commands"
---
The DESCRIBE command returns information about columns in a table or view.

## Syntax

The DESCRIBE command supports the following syntax:

    DESCRIBE [workspace.]table_name|view_name

## Usage Notes

You can issue the DESCRIBE command against views created in a workspace and
tables created in Hive, HBase, and MapR-DB. You can issue the DESCRIBE command
on a table or view from any schema. For example, if you are working in the
`dfs.myworkspace` schema, you can issue the DESCRIBE command on a view or
table in another schema. Currently, DESCRIBE does not support tables created
in a file system.

Drill only supports SQL data types. Verify that all data types in an external
data source, such as Hive or HBase, map to supported data types in Drill. See
Drill Data Type Mapping for more information.

## Example

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

