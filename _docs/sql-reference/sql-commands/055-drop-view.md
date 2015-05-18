---
title: "DROP VIEW"
parent: "SQL Commands"
---

The DROP VIEW command removes a view that was created in a workspace using the CREATE VIEW command.

## Syntax

The DROP VIEW command supports the following syntax:

     DROP VIEW [workspace.]view_name;

## Usage Notes

When you drop a view, all information about the view is deleted from the workspace in which it was created. DROP VIEW applies to the view only, not to the underlying data sources used to create the view. However, if you drop a view that another view is dependent on, you can no longer use the dependent view. If the underlying tables or views change after a view is created, you may want to drop and re-create the view. Alternatively, you can use the CREATE OR REPLACE VIEW syntax to update the view.

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
