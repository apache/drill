---
title: "CREATE TEMPORARY TABLE AS (CTTAS)"
date: 2017-03-27 18:12:49 UTC
parent: "SQL Commands"
---
As of Drill 1.10, you can use the CREATE TEMPORARY TABLE AS (CTTAS) command to store the results of a query in a temporary table. You can reference the temporary table in subsequent queries within the same session, thereby improving query performance. Data written to the temporary table is not permanently stored on the filesystem. Drill automatically drops the temporary table once the session ends or the Drillbit process fails. Therefore, you do not have to manually drop the table.

{% include startnote.html %}You cannot create a view over a temporary table.{% include endnote.html %}

## Syntax

    CREATE TEMPORARY TABLE name [ (column list) ][ PARTITION BY (column list) ] AS query

## Parameters ##
*name* - A directory name that is unique.  
  
*column list* - An optional list of column names or aliases in the new table. If specified, the number of columns for the temporary table must be the same as the number of columns in the SELECT statement of the query. When columns are not specified for the temporary table, the column names in the temporary table are set based on the source table column name or alias. If a column name has an alias, the alias is used.

[PARTITION BY]({{site.baseurl}}/docs/partition-by-clause) - An optional parameter that can only be used to create temporary tables with the Parquet data format. When used, it must specify the column or list of columns to be partitioned.

*query* - A SELECT statement that needs to include aliases for ambiguous column names, such as COLUMNS[0]. Using SELECT * is [not recommended]({{site.baseurl}}/docs/text-files-csv-tsv-psv/#tips-for-performant-querying) when selecting CSV, TSV, and PSV data.
  
## Usage Notes

###Workspace for Temporary Tables

By default, Drill creates temporary tables within the default temporary workspace, `dfs.tmp`. The default temporary workspace must be writable, file-based, and point to a location that already exists, otherwise temporary table creation fails. You cannot create a temporary table outside of the default temporary workspace.

Example dfs.tmp workspace in the dfs storage plugin:

	{
	"type": "file",
	"enabled": true,
	"connection": "maprfs:///",
	"config": null,
	"workspaces": {
	"root": {
	"location": "/",
	"writable": false,
	"defaultInputFormat": null
	},
	"tmp": {
	"location": "/tmp",
	"writable": true,
	"defaultInputFormat": null
	} ...
To override the default temporary workspace, define `drill.exec.default_temporary_workspace` in the `drill-override.conf file` and then restart the Drillbit. For more information, see [Configuration Option Introduction]({{site.baseurl}}/docs/configuration-options-introduction/#system-options).

To change the default temporary workspace connection or directory path, update the dfs storage plugin on the Storage page in the Drill Web Console. For example, dfs connection attribute can be `file:///` or `maprfs:///` and the `dfs.tmp` location attribute can be `/tmp` or `/tmp2`. For more information, see [Plugin Configuration Basics]({{site.baseurl}}/docs/plugin-configuration-basics).

Note: When you run Drill in distributed mode, verify that the default temporary workspace connection points to a distributed filesystem. If temporary tables are created on the local filesystem, they can only be accessed by the local Drillbit that created the temporary table.
###Setting the Storage Format
The default storage format for temporary tables is parquet. However, you can create temporary tables in one of the following formats:

* csv, tsv, psv
* parquet
* json 

To change the storage format, set the `store.format` option before you create the temporary table. For example, you can use the ALTER SESSION SET command to set the store.format option to JSON:

		ALTER SESSION SET `store.format`='json';

###Creation of Temporary Tables and User Access

In general, the user that creates a temporary table can run queries against the table, as long as the session in which the table was created is active. Although temporary tables are actually directories, you query the temporary table directory as you would query a table.

When you create a temporary table, Drill creates a temporary location named after the session ID to store temporary tables associated with the session. Drill writes temporary table files with masked filenames, such as 0_0_0.parquet, to temporary table directories within the session’s temporary location. Internally, Drill masks each temporary table directory name. Therefore, when you submit a query with the temporary table name, Drill resolves the temporary table name to the internal, masked directory name. 

Drill creates the session and temporary table directories and files with the following permissions:

* Only the owner can perform read, write, and execute operations on the folder directories.
* Only the owner can create and read the files within the folders.

###Authorization

When authorization is enabled, the user that started the Drillbit will own the temporary tables created within that session. However, the user that created the table will have the ability to query the data. For example, the *mapr* user starts the Drillbit and *Sally* starts the Drill shell. When authorization is enabled, *Sally* can submit a query to create a temporary table and she can query the temporary table data. However, the *mapr* user is the owner of the temporary table directory and files. In this case, *Tom* can access the temporary table if he knows the full path to the temporary table directory and the session is still active.

###Impersonation

When impersonation is enabled, only the user that created the temporary table can query the temporary table. For example, if the Drillbit was started by the *mapr* user and user *Tom* starts the Drill shell and creates a temporary table, *Tom* will have permission to query the temporary table. *Tom* can also access temporary tables that he created in a different session as long as he has the full path to the temporary table directory and the session is still active. However, *Sally* cannot query the temporary table even if she has the full path to the temporary table directory and the session is active.

###Selection of Tables

When you mention a table name in a SELECT statement, any temporary table with that name takes priority over a table with the same name in the current workspace.

For example, when you issue a SELECT statement on a table name that is common among the default temporary tables workspace and the current workspace, the temporary table is returned:

       USE dfs.json;

       SELECT * FROM donuts; //returns table from dfs.tmp  
         
       SELECT* FROM dfs.json.donuts; //returns table from dfs.json

###Drop a Temporary Table

Once a session ends or the drillbit process fails, Drill will drop the session’s temporary location which includes any associated temporary table directories and files. However, you can also drop a temporary table using the DROP TABLE command. When you drop a temporary table within a session, Drill deletes the temporary table directory and its files.

When you drop a temporary table, it is not required to specify the workspace. Any temporary table with the specified table name takes priority over a table with the same name in the current workspace.

In the following example, cust_promotions is the name of a persistent table under dfs.json and it is also the name of a temporary table.

	USE dfs.json; 
	DROP TABLE cust_promotions; 
	+-------+-----------------------------------------------------+
	|  ok   |                     summary                         |
	+-------+-----------------------------------------------------+
	| true  |      Temporary table [cust_promotions] dropped      |
	+-------+-----------------------------------------------------+

Even though the workspace was set to dfs.json, the temporary table from dfs.tmp was dropped.

##CTTAS Example

In the following example, the user has .csv file with the following data:

	"1","Bob","200","false"
	"2","Kate","150","true" 
	"3","Tim","20","false" 
	"4","Roger","500","true"

The first column is the customer id, the second column is the customer name, the third column is the sale amount, and the fourth column indicates if the customer got a discount:

The user wants to create a temporary table which sums the amount of sales with a discount and sums the amount of sales without a discount. The user runs the following command to create a table which has one column with the sales amount and another column which indicates if the sales were associated with a discount or not:

	CREATE TEMPORARY TABLE total_amount_by_discount as SELECT cast(columns[3] as boolean) discount, sum(cast(columns[2] as double)) total_amount FROM dfs.`/emp/amount.csv` GROUP BY columns[3];
		
    +--------------+-------------------------------------+ 
	|   Fragment   |      Number of records written      | 
	+-------------+--------------------------------------+ 
	| 0_0          |  2                                  | 
	+-------------+--------------------------------------+

The temporary table `total_amount_by_discount` was created in the default temporary workspace with the default storage format.

To view the temporary table, the user can issue a SELECT statement on the entire table:

	select * from total_amount_by_discount;

	+-----------+-------------------+ 
	| discount  | total_amount      |
	+-----------+-------------------+ 
	| false    | 220.0              |
	| true      | 650.0             | 
	+-----------+-------------------+

Within this session, the user can also query data from this temporary table using SELECT statements.