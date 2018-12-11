---
title: "DROP VIEW"
date: 2018-12-11
parent: "SQL Commands"
---

The DROP VIEW command removes a view that was created in a workspace using the CREATE VIEW command. As of Drill 1.8, you can include the IF EXISTS parameter with the DROP VIEW command.

## Syntax

The DROP VIEW command supports the following syntax:

    DROP VIEW [IF EXISTS] [workspace.]view_name;  

## Parameters  

IF EXISTS  
Drill does not throw an error if the view does not exist. Instead, Drill returns "`View [view_name] not found in schema [workspace].`"  

*workspace*  
The location of the view in subdirectories of a local or distributed file system.

*name*  
A unique directory or file name, optionally prefaced by a storage plugin name, such as `dfs`, and a workspace, such as `tmp` using dot notation. 

## Usage Notes  

- By default, Drill returns a result set when you issue DDL statements, such as DROP VIEW. If the client tool from which you connect to Drill (via JDBC) does not expect a result set when you issue DDL statements, set the `exec.return_result_set_for_ddl` option to false, as shown, to prevent the client from canceling queries:  

		SET `exec.return_result_set_for_ddl` = false  
		//This option is available in Drill 1.15 and later.   

	When set to false, Drill returns the affected rows count, and the result set is null.  



- When you drop a view, all information about the view is deleted from the workspace in which it was created. DROP VIEW applies to the view only, not to the underlying data sources used to create the view. However, if you drop a view that another view is dependent on, you can no longer use the dependent view. If the underlying tables or views change after a view is created, you may want to drop and re-create the view. Alternatively, you can use the CREATE OR REPLACE VIEW syntax to update the view.

## Example

This example shows you some steps to follow when you want to drop a view in Drill using the DROP VIEW command. A workspace named “donuts” was created for the steps in this example.  

Complete the following steps to drop a view in Drill:  

Use the writable workspace from which the view was created.

    0: jdbc:drill:zk=local> use dfs.donuts;
    +------------+------------+
    |     ok    |  summary   |
    +------------+------------+
    | true      | Default schema changed to 'dfs.donuts' |
    +------------+------------+
 
Use the DROP VIEW command to remove a view created in the current workspace.

    0: jdbc:drill:zk=local> drop view mydonuts;
    +------------+------------+
    |     ok    |  summary   |
    +------------+------------+
    | true      | View 'mydonuts' deleted successfully from 'dfs.donuts' schema |
    +------------+------------+

Use the DROP VIEW command to remove a view created in another workspace.

    0: jdbc:drill:zk=local> drop view dfs.tmp.yourdonuts;
    +------------+------------+
    |   ok  |  summary   |
    +------------+------------+
    | true      | View 'yourdonuts' deleted successfully from 'dfs.tmp' schema |  
    +------------+------------+  

Use the DROP VIEW command with or without the IF EXISTS parameter to remove a view created in the current workspace.  

    0: jdbc:drill:zk=local> drop view if exists mydonuts;
    +------------+--------------------------------------------------------------+
    |     ok     | summary                                                      |
    +------------+--------------------------------------------------------------+
    | true       | View 'mydonuts' deleted successfully from 'dfs.donuts' schema|
    +------------+--------------------------------------------------------------+

Use the DROP VIEW command with the IF EXISTS parameter to remove a view that does not exist in the current workspace, either because it was never created or it was already removed.

    0: jdbc:drill:zk=local> drop view if exists mydonuts;
    +-------+---------------------------------------------------+
    |  ok   |                   summary                         |
    +-------+---------------------------------------------------+
    | true  | View 'mydonuts' not found in schema 'dfs.donuts'  |
    +-------+---------------------------------------------------+
    1 row selected (0.085 seconds)

