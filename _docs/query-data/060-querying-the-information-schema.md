---
title: "Querying the INFORMATION SCHEMA"
date: 2018-11-07
parent: "Query Data"
---  

When you are using Drill to connect to multiple data sources, you need a
simple mechanism to discover what each data source contains. The information
schema is an ANSI standard set of metadata tables that you can query to return
information about all of your Drill data sources (or schemas). Data sources
may be databases or file systems; they are all known as "schemas" in this
context. You can query the following INFORMATION_SCHEMA tables:

  * SCHEMATA
  * CATALOGS
  * TABLES
  * COLUMNS 
  * VIEWS
  * FILES

## SCHEMATA

The SCHEMATA table contains the CATALOG_NAME and SCHEMA_NAME columns. To allow
maximum flexibility inside BI tools, the only catalog that Drill supports is
`DRILL`.

    SELECT CATALOG_NAME, SCHEMA_NAME as all_my_data_sources FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME;
    +--------------+---------------------+
    | CATALOG_NAME | all_my_data_sources |
    +--------------+---------------------+
    | DRILL        | INFORMATION_SCHEMA  |
    | DRILL        | cp.default          |
    | DRILL        | dfs.default         |
    | DRILL        | dfs.root            |
    | DRILL        | dfs.tmp             |
    | DRILL        | HiveTest.SalesDB    |
    | DRILL        | maprfs.logs         |
    | DRILL        | sys                 |
    +--------------+---------------------+

The INFORMATION_SCHEMA name and associated keywords are case-insensitive. You
can also return a list of schemas by running the SHOW DATABASES command:

    SHOW DATABASES;
    +-------------+
    | SCHEMA_NAME |
    +-------------+
    | dfs.default |
    | dfs.root    |
    | dfs.tmp     |
    ...

## CATALOGS

The CATALOGS table returns only one row, with the hard-coded DRILL catalog name
and description.

## TABLES

The TABLES table returns the table name and type for each table or view in
your databases. (Type means TABLE or VIEW.) Starting in Drill 1.15, Drill returns
files available for querying in file-based data sources. You no longer have to use the SHOW
FILES command to explore these data sources. You can query the FILES table for directory and 
file information. 

## COLUMNS

The COLUMNS table returns the column name and other metadata (such as the data
type) for each column in each table or view.

## VIEWS

The VIEWS table returns the name and definition for each view in your
databases. Note that file schemas are the canonical repository for views in
Drill. Depending on how you create a view, the may only be displayed in Drill
after it has been used.  

## FILES

Starting in Drill 1.15, the INFORMATION_SCHEMA contains a FILES table that you can query for information about directories and files stored in the [workspaces]({{site.baseurl}}/docs/workspaces/) configured within your [S3]({{site.baseurl}}/docs/s3-storage-plugin/#configuring-the-s3-storage-plugin) and [file system]({{site.baseurl}}/docs/file-system-storage-plugin/) storage plugin configurations. 

The FILES table is useful for analyzing folders and files before you run queries against data sources configured in Drill. When you query the FILES table, the FILES table lists the directories and files based on the permissions set for the current or impersonated user.  

The FILES table stores the following information about directories and files in workspaces:  

- **SCHEMA_NAME**  
The file system storage plugin name with the schema name. For example, dfs.tmp.  
- **ROOT\_SCHEMA\_NAME**  
The file system storage plugin name. For example, dfs.  
- **WORKSPACE_NAME**  
The workspace name. For example, tmp.  
* **FILE_NAME**  
The name of the directories and files. For example, sample.txt. Drill lists directories and files based on the permissions set for the current or impersonated user.    
* **RELATIVE\_PATH**  
The relative path to a file. For example, `sample_folder/sample.txt`; assuming that the full file path is `/tmp/sample_folder/sample.txt`, and the workspace path is `/tmp`.  
* **IS_DIRECTORY**  
Lists true if the object is a directory. Lists false if the object is a file.  
* **IS_FILE**   
Lists true if the object is a file. Lists false if the object is a directory.    
* **LENGTH**  
Size of the directory or file in bytes. For example, 1210.  
* **OWNER**  
File or directory owner. For example, root.  
* **GROUP**  
Group to which the file or directory belongs. For example, root.  
* **PERMISSION**  
Permission that the current or impersonated user has on the file. For example, rw-rw-rw.  
* **ACCESS_TIME**  
Timestamp denoting the last time the file or directory was accessed.   
* **MODIFICATION_TIME**  
Timestamp denoting the last time the file or directory was changed.  

### Listing Files Recursively  

The FILES table can list files recursively; however, listing files recursively can negatively impact performance. When you enable the `storage.list_files_recursively` option, the FILES table lists all the directories and files nested under the current workspace directory. The `storage.list_files_recursively` option is disabled (set to false) by default. Issue the SET command to enable recursive listing, as shown:  

	SET `storage.list_files_recursively` = true;      

## Useful Queries  

The following sections demonstrate how to query the FILES table and TABLES table in the INFORMATION_SCHEMA:  

### FILES Queries  
This example demonstrates how to use the FILES table to explore workspaces and identify duplicate files across the workspaces that are configured in different S3 storage plugins. 

For this example, S3 buckets were configured as data sources in Drill. Storage plugins were configured to connect Drill to each of the S3 buckets. The storage plugin named `s3_home_bucket` contains personal files, and the storage plugin named `s3_work_bucket` contains work files. Naming the storage plugins with the s3 prefix simplifies the listing of available schemas in the SCHEMATA table, as shown:  

	0: jdbc:drill:zk=local> select * from information_schema.schemata where schema_name like 's3%';
	+---------------+--------------------------+---------------+-------+-------------+
	| CATALOG_NAME  |       SCHEMA_NAME        | SCHEMA_OWNER  | TYPE  | IS_MUTABLE  |
	+---------------+--------------------------+---------------+-------+-------------+
	| DRILL         | s3_home_bucket.default   | <owner>       | file  | NO          |
	| DRILL         | s3_home_bucket.root      | <owner>       | file  | NO          |
	| DRILL         | s3_work_bucket.default   | <owner>       | file  | NO          |
	| DRILL         | s3_work_bucket.root      | <owner>       | file  | NO          |
	| DRILL         | s3_years_bucket.default  | <owner>       | file  | NO          |
	| DRILL         | s3_years_bucket.root     | <owner>       | file  | NO          |
	+---------------+--------------------------+---------------+-------+-------------+  

Querying the FILES table and filtering on the SCHEMA_NAME provides information about the files that exist within a workspace:  

**Note:** The word “files” is a reserved word in Drill and requires backticks (``).   

	0: jdbc:drill:zk=local> select * from information_schema.`files` where schema_name = 's3_home_bucket.root';
	+----------------------+-------------------+-----------------+-----------------------------+-----------------------------+---------------+----------+---------+--------+--------+-------------+------------------------+------------------------+
	|     SCHEMA_NAME      | ROOT_SCHEMA_NAME  | WORKSPACE_NAME  |          FILE_NAME          |        RELATIVE_PATH        | IS_DIRECTORY  | IS_FILE  | LENGTH  | OWNER  | GROUP  | PERMISSION  |      ACCESS_TIME       |   MODIFICATION_TIME    |
	+----------------------+-------------------+-----------------+-----------------------------+-----------------------------+---------------+----------+---------+--------+--------+-------------+------------------------+------------------------+
	| s3_home_bucket.root  | s3_home_bucket    | root            | date_dim.txt                | date_dim.txt                | false         | true     | 0       | root   | root   | rw-r--r--   | 1969-12-31 16:00:00.0  | 2018-11-06 16:38:16.0  |
	| s3_home_bucket.root  | s3_home_bucket    | root            | household_demographics.txt  | household_demographics.txt  | false         | true     | 0       | root   | root   | rw-r--r--   | 1969-12-31 16:00:00.0  | 2018-11-06 16:38:28.0  |
	| s3_home_bucket.root  | s3_home_bucket    | root            | promotion.txt               | promotion.txt               | false         | true     | 0       | root   | root   | rw-r--r--   | 1969-12-31 16:00:00.0  | 2018-11-06 16:38:35.0  |
	| s3_home_bucket.root  | s3_home_bucket    | root            | time_dim.txt                | time_dim.txt                | false         | true     | 0       | root   | root   | rw-r--r--   | 1969-12-31 16:00:00.0  | 2018-11-06 16:38:43.0  |
	+----------------------+-------------------+-----------------+-----------------------------+-----------------------------+---------------+----------+---------+--------+--------+-------------+------------------------+------------------------+  

	0: jdbc:drill:zk=local> select * from information_schema.`files` where schema_name = 's3_work_bucket.root';
	+----------------------+-------------------+-----------------+-----------------------------+-----------------------------+---------------+----------+---------+--------+--------+-------------+------------------------+------------------------+
	|     SCHEMA_NAME      | ROOT_SCHEMA_NAME  | WORKSPACE_NAME  |          FILE_NAME          |        RELATIVE_PATH        | IS_DIRECTORY  | IS_FILE  | LENGTH  | OWNER  | GROUP  | PERMISSION  |      ACCESS_TIME       |   MODIFICATION_TIME    |
	+----------------------+-------------------+-----------------+-----------------------------+-----------------------------+---------------+----------+---------+--------+--------+-------------+------------------------+------------------------+
	| s3_work_bucket.root  | s3_work_bucket    | root            | customer.txt                | customer.txt                | false         | true     | 0       | root   | root   | rw-r--r--   | 1969-12-31 16:00:00.0  | 2018-11-06 16:55:36.0  |
	| s3_work_bucket.root  | s3_work_bucket    | root            | household_demographics.txt  | household_demographics.txt  | false         | true     | 0       | root   | root   | rw-r--r--   | 1969-12-31 16:00:00.0  | 2018-11-06 16:55:59.0  |
	| s3_work_bucket.root  | s3_work_bucket    | root            | item.txt                    | item.txt                    | false         | true     | 0       | root   | root   | rw-r--r--   | 1969-12-31 16:00:00.0  | 2018-11-06 16:56:13.0  |
	| s3_work_bucket.root  | s3_work_bucket    | root            | promotion.txt               | promotion.txt               | false         | true     | 0       | root   | root   | rw-r--r--   | 1969-12-31 16:00:00.0  | 2018-11-06 16:56:29.0  |
	+----------------------+-------------------+-----------------+-----------------------------+-----------------------------+---------------+----------+---------+--------+--------+-------------+------------------------+------------------------+

Notice that the FILE\_NAME column lists the files stored in the workspaces. You can see that duplicate files exist in the work and home buckets. Alternatively, you can see the duplicate files by querying the FILE\_NAME column directly and filtering on SCHEMA\_NAME and IS\_FILE, as shown:  

	0: jdbc:drill:zk=local> select file_name from information_schema.`files` where schema_name = 's3_home_bucket.root' and is_file is true;
	+-----------------------------+
	|          file_name          |
	+-----------------------------+
	| date_dim.txt                |
	| household_demographics.txt  |
	| promotion.txt               |
	| time_dim.txt                |
	+-----------------------------+

	0: jdbc:drill:zk=local> Select file_name from information_schema.`files` where schema_name = 's3_work_bucket.root' and is_file is true;
	+-----------------------------+
	|          file_name          |
	+-----------------------------+
	| customer.txt                |
	| household_demographics.txt  |
	| item.txt                    |
	| promotion.txt               |
	+-----------------------------+  

Issuing a slightly more complex query on the FILES table reveals the duplicate files across the two schemas:  

	0: jdbc:drill:zk=local> select file_name from information_schema.`files` where schema_name in ('s3_work_bucket.root', 's3_home_bucket.root') and is_file is true group by file_name having count(file_name) > 1;
	+-----------------------------+
	|          file_name          |
	+-----------------------------+
	| household_demographics.txt  |
	| promotion.txt               |
	+-----------------------------+  

By default, the FILES table does not list the files recursively. Another schema named `s3_years_bucket.root` contains three folders with files in it, as shown:  

	0: jdbc:drill:zk=local> Select file_name, is_directory from information_schema.`files` where schema_name = 's3_years_bucket.root';
	+------------+---------------+
	| file_name  | is_directory  |
	+------------+---------------+
	| 2016       | true          |
	| 2017       | true          |
	| 2018       | true          |
	+------------+---------------+  

Though the folders contain files, the FILES table does not list the files nested inside the folders unless we enable the `storage.list_files_recursively` option, as shown:  

	0: jdbc:drill:zk=local> SET `storage.list_files_recursively` = true;
	+-------+------------------------------------------+
	|  ok   |                 summary                  |
	+-------+------------------------------------------+
	| true  | storage.list_files_recursively updated.  |
	+-------+------------------------------------------+  

With recursive listing enabled, you can see that the same query run against the schema reveals the nested files in the folders:  

	0: jdbc:drill:zk=local> select file_name, relative_path, is_directory, is_file from information_schema.`files` where schema_name = 's3_years_bucket.root';
	+--------------------------+-------------------------------+---------------+----------+
	|        file_name         |         relative_path         | is_directory  | is_file  |
	+--------------------------+-------------------------------+---------------+----------+
	| 2016                     | 2016                          | true          | false    |
	| profile_2016_01_01.json  | 2016/profile_2016_01_01.json  | false         | true     |
	| 2017                     | 2017                          | true          | false    |
	| profile_2017_01_01.json  | 2017/profile_2017_01_01.json  | false         | true     |
	| 2018                     | 2018                          | true          | false    |
	| profile_2018_01_01.json  | 2018/profile_2018_01_01.json  | false         | true     |
	+--------------------------+-------------------------------+---------------+----------+

### TABLES Queries

Run an ``INFORMATION_SCHEMA.`TABLES` ``query to view all of the tables and views
within a database. TABLES is a reserved word in Drill and requires back ticks
(`).

For example, the following query identifies all of the tables and views that
Drill can access:

    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
    FROM INFORMATION_SCHEMA.`TABLES`
    ORDER BY TABLE_NAME DESC;
    ----------------------------------------------------------------
    TABLE_SCHEMA             TABLE_NAME            TABLE_TYPE
    ----------------------------------------------------------------
    HiveTest.CustomersDB     Customers             TABLE
    HiveTest.SalesDB         Orders                TABLE
    HiveTest.SalesDB         OrderLines            TABLE
    HiveTest.SalesDB         USOrders              VIEW
    dfs.default              CustomerSocialProfile VIEW
    ----------------------------------------------------------------

{% include startnote.html %}Currently, Drill only supports querying Drill views; Hive views are not yet supported.{% include endnote.html %}

You can run a similar query to identify columns in tables and the data types
of those columns:

    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'Orders' AND TABLE_SCHEMA = 'HiveTest.SalesDB' AND COLUMN_NAME LIKE '%Total';
    +-------------+------------+
    | COLUMN_NAME | DATA_TYPE  |
    +-------------+------------+
    | OrderTotal  | Decimal    |
    +-------------+------------+  

