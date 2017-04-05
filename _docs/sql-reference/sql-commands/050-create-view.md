---
title: "CREATE VIEW"
date: 2017-04-05 00:09:58 UTC
parent: "SQL Commands"
---
The CREATE VIEW command creates a virtual structure for the result set of a
stored query. A view can combine data from multiple underlying data sources
and provide the illusion that all of the data is from one source. You can use
views to protect sensitive data, for data aggregation, and to hide data
complexity from users. You can create Drill views from files in your local and
distributed file systems, such as Hive and HBase tables, as well as from
existing views or any other available storage plugin data sources.

## Syntax

The CREATE VIEW command supports the following syntax:

    CREATE [OR REPLACE] VIEW [workspace.]view_name [ (column_name [, ...]) ] AS query;

Use CREATE VIEW to create a new view. Use CREATE OR REPLACE VIEW to replace an
existing view with the same name. When you replace a view, the query must
generate the same set of columns with the same column names and data types.

**Note:** Follow Drill’s rules for identifiers when you name the view. 

## Parameters

_workspace_  
The location where you want the view to exist. By default, the view is created
in the current workspace. See
[Workspaces]({{ site.baseurl }}/docs/workspaces/).

_view_name_  
The name that you give the view. The view must have a unique name. It cannot
have the same name as any other view or table in the workspace.

_column_name_  
Optional list of column names in the view. If you do not supply column names,
they are derived from the query.

_query_  
A SELECT statement that defines the columns and rows in the view.

## Usage Notes

### Storage

Drill stores views in the location specified by the workspace that you use
when you run the CREATE VIEW command. If the workspace is not defined, Drill
creates the view in the current workspace. You must use a writable workspace
when you create a view. Currently, Drill only supports views created in the
file system or distributed file system.

The following example shows a writable workspace as defined within the storage
plugin in the `/tmp` directory of the file system:

    "tmp": {
          "location": "/tmp",
          "writable": true,
           }

Drill stores the view definition in JSON format with the name that you specify
when you run the CREATE VIEW command, suffixed `by .view.drill`. For example,
if you create a view named `myview`, Drill stores the view in the designated
workspace as `myview.view.drill`.

  
## Related Commands

After you create a view using the CREATE VIEW command, you can issue the
following commands against the view:

  * SELECT 
  * DESCRIBE 
  * DROP 

{% include startnote.html %}You cannot update, insert into, or delete from a view.{% include endnote.html %}

## Example

This example shows you some steps that you can follow when you want to create
a view in Drill using the CREATE VIEW command. A workspace named “donuts” was
created for the steps in this example.

Complete the following steps to create a view in Drill:

  1. Decide which workspace you will use to create the view, and verify that the writable option is set to “true.” You can use an existing workspace, or you can create a new workspace. See [Workspaces]({{site.baseurl}}/docs/workspaces/) for more information.  
  
        "workspaces": {
           "donuts": {
             "location": "/home/donuts",
             "writable": true,
             "defaultInputFormat": null
           }
         },

  2. Run SHOW DATABASES to verify that Drill recognizes the workspace.  

        0: jdbc:drill:zk=local> show databases;
        +-------------+
        | SCHEMA_NAME |
        +-------------+
        | dfs.default |
        | dfs.root  |
        | dfs.donuts  |
        | dfs.tmp   |
        | cp.default  |
        | sys       |
        | INFORMATION_SCHEMA |
        +-------------+

  3. Use the writable workspace.  

        0: jdbc:drill:zk=local> use dfs.donuts;
        +------------+------------+
        |     ok    |  summary   |
        +------------+------------+
        | true      | Default schema changed to 'dfs.donuts' |
        +------------+------------+

  4. Test run the query that you plan to use with the CREATE VIEW command.  

        0: jdbc:drill:zk=local> select id, type, name, ppu from `donuts.json`;
        +------------+------------+------------+------------+
        |     id    |   type    |   name    |    ppu    |
        +------------+------------+------------+------------+
        | 0001      | donut      | Cake     | 0.55      |
        +------------+------------+------------+------------+

  5. Run the CREATE VIEW command with the query.  

        0: jdbc:drill:zk=local> create view mydonuts as select id, type, name, ppu from `donuts.json`;
        +------------+------------+
        |     ok    |  summary   |
        +------------+------------+
        | true      | View 'mydonuts' created successfully in 'dfs.donuts' schema |
        +------------+------------+

  6. Create a new view in another workspace from the current workspace.  

        0: jdbc:drill:zk=local> create view dfs.tmp.yourdonuts as select id, type, name from `donuts.json`;
        +------------+------------+
        |   ok  |  summary   |
        +------------+------------+
        | true      | View 'yourdonuts' created successfully in 'dfs.tmp' schema |
        +------------+------------+

  7. Query the view created in both workspaces.

        0: jdbc:drill:zk=local> select * from mydonuts;
        +------------+------------+------------+------------+
        |     id    |   type    |   name    |    ppu    |
        +------------+------------+------------+------------+
        | 0001      | donut      | Cake     | 0.55      |
        +------------+------------+------------+------------+
         
         
        0: jdbc:drill:zk=local> select * from dfs.tmp.yourdonuts;
        +------------+------------+------------+
        |   id  |   type    |   name    |
        +------------+------------+------------+
        | 0001      | donut     | Cake      |
        +------------+------------+------------+
