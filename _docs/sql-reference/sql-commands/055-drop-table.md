---
title: "DROP TABLE"
parent: "SQL Commands"
---

As of Drill 1.2, you can use the DROP TABLE command to remove tables (files or directories) from a file system when the file system is configured as a DFS storage plugin. See [Storage Plugin Registration]({{ site.baseurl }}/docs/storage-plugin-registration/). Currently, you can only issue the DROP TABLE command against file system data sources.

## Syntax
The DROP TABLE command supports the following syntax: 

       DROP TABLE [workspace.]name;

*name* is a unique directory or file name, optionally prefaced by a storage plugin name, such as dfs, and a workspace, such as tmp using dot notation.


## Usage Notes

###Schema
* You must identify the schema in which a table exists to successfully drop the table. You can identify the schema before dropping the table with the USE <schema_name> command (see [USE command]({{ site.baseurl }}/docs/use/)) or when you issue the DROP TABLE command. See [Example 1: Identifying a schema]({{ site.baseurl }}/docs/drop-table/#example-1:-identifying-a-schema).  
* The schema must be mutable. For example, to drop a table from a schema named `dfs.sales`, the "`writable`" attribute for the sales workspace in the DFS storage plugin configuration must be set to `true`. See [Storage Plugin Attributes]({{ site.baseurl }}/docs/plugin-configuration-basics/#storage-plugin-attributes). 

###File Type
* The DROP TABLE command only works against file types that Drill can read. File types are identified as supported file formats, such as Parquet, JSON, or text. See [Querying a File System Introduction]({{ site.baseurl }}/docs/querying-a-file-system-introduction/) for a complete list of supported types. 
* Text formats must be configured in the DFS storage plugin configuration. For example, to support CSV files, the “`format`” attribute in the configuration must include CSV as a value. See [Storage Plugin Attributes]({{ site.baseurl }}/docs/plugin-configuration-basics/#storage-plugin-attributes).
* The directory on which you issue the DROP TABLE command must contain files of the same type. For example, if you have a workspace configured, such as `dfs.sales`, that points to a directory containing subdirectories, such as `/2012` and `/2013`, files in all of the directories must be of the same type in order to successfully issue the DROP TABLE command against the directory.  

###Permissions
* A user must have the appropriate permissions on the file system to successfully issue the DROP TABLE command. Inadequate permissions result in a failed drop and can potentially remove a subset of the files in a directory.  

###User Impersonation
* When user impersonation is enabled in Drill, Drill impersonates the user issuing the DROP TABLE command. Therefore, the user must have sufficient permissions on the file system for the command to succeed. See [Configuring User Impersonation]({{ site.baseurl }}/docs/configuring-user-impersonation/).
* When user impersonation is not enabled in Drill, Drill accesses the file system as the user running the Drillbit. This user is typically a super user who has permission to delete most files. In this scenario, use the DROP TABLE command with caution to avoid deleting critical files and directories.  

###Views
* Views are independent of tables. Views that reference dropped tables become invalid. You must explicitly drop any view that references a dropped table using the [DROP VIEW command]({{ site.baseurl }}/docs/drop-view/).  

###Concurrency 
* Concurrency occurs when two processes try to access and/or change data at the same time. Currently, Drill does not have a mechanism in place, such as read locks on files, to address concurrency issues. For example, if one user runs a query that references a table that another user simultaneously issues the DROP TABLE command against, there is no mechanism in place to prevent a collision of the two processes. In such a scenario, Drill may return partial query results or a system error to the user running the query when the table is dropped. 


## Examples

The following examples show results for several DROP TABLE scenarios.  

###Example 1:  Identifying a schema  
This example shows you how to identify a schema with the USE and DROP TABLE commands to successfully drop a table named `donuts_json` in the “`donuts`” workspace configured within the DFS storage plugin configuration.  

The "`donuts`" workspace is configured within the following DFS configuration:  

        {
         "type": "file",
         "enabled": true,
         "connection": "file:///",
         "workspaces": {
           "root": {
             "location": "/",
             "writable": false,
             "defaultInputFormat": null
           },
           "donuts": {
             "location": "/Users/user1/donuts",
             "writable": true,
             "defaultInputFormat": null
           }
         },

Issuing the `USE dfs.donuts` command changes to the `dfs.donuts` schema before issuing the `DROP TABLE` command.

       0: jdbc:drill:zk=local> use dfs.donuts;
       +-------+-----------------------------------------+
       |  ok   |                 summary                 |
       +-------+-----------------------------------------+
       | true  | Default schema changed to [dfs.donuts]  |
       +-------+-----------------------------------------+
       1 row selected (0.096 seconds)
        
       0: jdbc:drill:zk=local> drop table donuts_json;
       +-------+------------------------------+
       |  ok   |           summary            |
       +-------+------------------------------+
       | true  | Table [donuts_json] dropped  |
       +-------+------------------------------+
       1 row selected (0.094 seconds) 

Alternatively, instead of issuing the `USE` command to change the schema, you can include the schema name when you drop the table.

       0: jdbc:drill:zk=local> drop table dfs.donuts.donuts_json;
       +-------+------------------------------+
       |  ok   |           summary            |
       +-------+------------------------------+
       | true  | Table [donuts_json] dropped  |
       +-------+------------------------------+
       1 row selected (1.189 seconds)

Drill returns the following error when the schema is not identified:

       0: jdbc:drill:zk=local> drop table donuts_json;

       Error: PARSE ERROR: Root schema is immutable. Creating or dropping tables/views is not allowed in root schema.Select a schema using 'USE schema' command.
       [Error Id: 8c42cb6a-27eb-48fd-b42a-671a6fb58c14 on 10.250.56.218:31010] (state=,code=0)
       
###Example 2: Dropping a table created from a file
In the following example, the `donuts_json` table is removed from the `/tmp` workspace using the `DROP TABLE` command. This example assumes that the steps in the [Complete CTAS Example]({{ site.baseurl }}/docs/create-table-as-ctas/#complete-ctas-example) were already completed. 

Running an `ls` on the `/tmp` directory shows the `donuts_json` file.

       $ pwd
       /tmp
       $ cd donuts_json
       $ ls
       0_0_0.json
       $ more 0_0_0.json
       {
        "id" : "0001",
         "type" : "donut",
         "name" : "Cake",
         "ppu" : 0.55
       }  
Issuing `USE dfs.tmp` changes schema.  

       0: jdbc:drill:zk=local> use dfs.tmp;
       +-------+-----------------------------------------+
       |  ok   |                 summary            	 |
       +-------+-----------------------------------------+
       | true  | Default schema changed to [dfs.tmp]  |
       +-------+-----------------------------------------+
       1 row selected (0.085 seconds)  

Running the `DROP TABLE` command removes the table from the schema.
       
       0: jdbc:drill:zk=local> drop table donuts_json;
       +-------+------------------------------+
       |  ok   |           summary            |
       +-------+------------------------------+
       | true  | Table [donuts_json] dropped  |
       +-------+------------------------------+
       1 row selected (0.107 seconds)  

###Example 3: Dropping a table created as a directory  
When you create a table that writes files to a directory, you can issue the `DROP TABLE` command against the table to remove the directory. All files and subdirectories are deleted. For example, the following `CTAS` command writes Parquet data from the `nation.parquet` file, installed with Drill, to the `/tmp/name_key` directory.  

Issue the `USE` command to change schema.  
              
       0: jdbc:drill:zk=local> USE dfs;

Create a table using the `CTAS` command.

       0: jdbc:drill:zk=local> CREATE TABLE tmp.`name_key` (N_NAME, N_NATIONKEY) AS SELECT N_NATIONKEY, N_NAME FROM dfs.`/Users/drilluser/apache-drill-1.2.0/sample-data/nation.parquet`;
       +-----------+----------------------------+
       | Fragment  | Number of records written  |
       +-----------+----------------------------+
       | 0_0       | 25                         |
       +-----------+----------------------------+
       Query the directory to see the data.

Query the directory to see the data. 

       0: jdbc:drill:zk=local> select * from tmp.`name_key`;
       +---------+-----------------+
       | N_NAME  |   N_NATIONKEY   |
       +---------+-----------------+
       | 0       | ALGERIA         |
       | 1       | ARGENTINA       |
       | 2       | BRAZIL          |
       | 3       | CANADA          |
       | 4       | EGYPT           |
       | 5       | ETHIOPIA        |
       | 6       | FRANCE          |
       | 7       | GERMANY         |
       | 8       | INDIA           |
       | 9       | INDONESIA       |
       | 10      | IRAN            |
       | 11      | IRAQ            |
       | 12      | JAPAN           |
       | 13      | JORDAN          |
       | 14      | KENYA           |
       | 15      | MOROCCO         |
       | 16      | MOZAMBIQUE      |
       | 17      | PERU            |
       | 18      | CHINA           |
       | 19      | ROMANIA         |
       | 20      | SAUDI ARABIA    |
       | 21      | VIETNAM         |
       | 22      | RUSSIA          |
       | 23      | UNITED KINGDOM  |
       | 24      | UNITED STATES   |
       +---------+-----------------+
       25 rows selected (0.183 seconds)

Issue the `DROP TABLE` command against the directory to remove the directory and deletes all files and subdirectories that existed within the directory.

       0: jdbc:drill:zk=local> drop table name_key;
       +-------+---------------------------+
       |  ok   |          summary          |
       +-------+---------------------------+
       | true  | Table [name_key] dropped  |
       +-------+---------------------------+
       1 row selected (0.086 seconds)

###Example 4: Dropping a table that does not exist
The following example shows the result of dropping a table that does not exist because it has already been dropped or it never existed. 

       0: jdbc:drill:zk=local> use use dfs.tmp;
       +-------+--------------------------------------+
       |  ok   |               summary                |
       +-------+--------------------------------------+
       | true  | Default schema changed to [dfs.tmp]  |
       +-------+--------------------------------------+
       1 row selected (0.289 seconds)
       
       0: jdbc:drill:zk=local> drop table name_key;

       Error: VALIDATION ERROR: Table [name_key] not found
       [Error Id: fc6bfe17-d009-421c-8063-d759d7ea2f4e on 10.250.56.218:31010] (state=,code=0)

###Example 5: Dropping a table without permissions 
The following example shows the result of dropping a table without appropriate permissions in the file system.

       0: jdbc:drill:zk=local> drop table name_key;

       Error: PERMISSION ERROR: Unauthorized to drop table
       [Error Id: 36f6b51a-786d-4950-a4a7-44250f153c55 on 10.10.30.167:31010] (state=,code=0)  

###Example 6: Dropping and querying a table concurrently  

The result of this scenario depends on the delta in time between one user dropping a table and another user issuing a query against the table. Results can also vary. In some instances the drop may succeed and the query fails completely or the query completes partially and then the table is dropped returning an exception in the middle of the query results.

The following example shows the result of dropping a table and issuing a query against the table simultaneously. In this example, the table is dropped before the query can run against it. 

**User 1 issues the DROP TABLE command.**  
       
       0: jdbc:drill:zk=local> drop table name_key;
       +-------+------------------------------+
       |  ok   |           summary            |
       +-------+------------------------------+
       | true  | Table [droptable34] dropped  |
       +-------+------------------------------+
       1 row selected (12.35 seconds)

**User 2 issues a query against the table.**  
       
       0: jdbc:drill:zk=local> select * from name_key;

       Error: SYSTEM ERROR: FileNotFoundException: Requested file does not exist.
       Fragment 1:0
       [Error Id: 6e3c6a8d-8cfd-4033-90c4-61230af80573 on 10.10.30.167:31010] (state=,code=0)

###Example 7: Dropping a table with different file formats
The following example shows the result of dropping a table when multiple file formats exists in the directory. In this scenario, the `sales_dir` table resides in the `dfs.sales` workspace and contains Parquet, CSV, and JSON files.

Running `ls` on `sales_dir` shows that different file types exist in the directory.

       $ cd sales_dir/
       $ ls
       0_0_0.parquet	sales_a.csv	sales_b.json	sales_c.parquet

Issuing the `DROP TABLE` command on the directory results in an error.

       0: jdbc:drill:zk=local> drop table dfs.sales.sales_dir;

       Error: VALIDATION ERROR: Table contains different file formats. 
       Drop Table is only supported for directories that contain homogeneous file formats consumable by Drill
       [Error Id: 062f68c9-f2cd-4033-9b3d-182146a96904 on 10.250.56.218:31010] (state=,code=0)




        


