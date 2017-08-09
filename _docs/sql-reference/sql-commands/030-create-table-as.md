---
title: "CREATE TABLE AS (CTAS)"
date: 2017-08-09 22:58:36 UTC
parent: "SQL Commands"
---
Use the CTAS command to create tables in Drill.

## Syntax

    CREATE TABLE name [ (column list) ] AS query;  

## Parameters
*name*  
A unique directory name, optionally prefaced by a storage plugin name, such as dfs, and a workspace, such as tmp using [dot notation]({{site.baseurl}}/docs/workspaces).  
  
*column list*  
An optional list of column names or aliases in the new table.  
  
*query*  
A SELECT statement that needs to include aliases for ambiguous column names, such as COLUMNS[0]. Using SELECT * is [not recommended]({{site.baseurl}}/docs/text-files-csv-tsv-psv/#tips-for-performant-querying) when selecting CSV, TSV, and PSV data.  

## Usage Notes  

- You can use the [PARTITION BY]({{site.baseurl}}/docs/partition-by-clause) clause in a CTAS command.  

- Drill writes files having names, such as 0_0_0.parquet, to the directory named in the CTAS command or to the workspace that is in use when you run the CTAS statement. You query the directory as you would query a table.



       - The following command writes Parquet data from `nation.parquet`, installed with Drill, to the `/tmp/name_key` directory.

              USE dfs;
              CREATE TABLE tmp.`name_key` (N_NAME, N_NATIONKEY) AS SELECT N_NATIONKEY, N_NAME FROM dfs.`/Users/drilluser/apache-drill-1.0/sample-data/nation.parquet`;  


       - To query the data, use this command:

              SELECT * FROM tmp.`name_key`;



       - This example writes a JSON table to the `/tmp/by_yr` directory that contains [Google Ngram data]({{site.baseurl}}/docs/partition-by-clause/#partioning-example).

              Use dfs.tmp;
              CREATE TABLE by_yr (yr, ngram, occurrances) AS SELECT COLUMNS[0] ngram, COLUMNS[1] yr, COLUMNS[2] occurrances FROM `googlebooks-eng-all-5gram-20120701-zo.tsv` WHERE (columns[1] = '1993');



- Drill 1.11 introduces the `exec.persistent_table.umask` option, which enables you to set permissions on tables and directories that result from running the CTAS command. By default, the option is set to 002, which sets the default directory permissions to 775 and default file permissions to 664. Use the [SET]({{site.baseurl}}/docs/set/) command to change the setting for this option at the system or session level, as shown:  

        ALTER SYSTEM|SESSION SET `exec.persistent_table.umask` = '000';  



       - Setting the option to '000' sets the folder permissions to 777 and the file permissions to 666. This setting gives full access to folders and files when you create a table.



## Setting the Storage Format

Before using CTAS, set the `store.format` option for the table to one of the following formats:

  * csv, tsv, psv
  * parquet
  * json

Use the ALTER SESSION command as [shown in the example]({{site.baseurl}}/docs/create-table-as-ctas/#alter-session-command) in this section to set the `store.format` option.

## Restrictions

You can only create new tables in workspaces. You cannot create tables using
storage plugins, such as Hive and HBase, that do not specify a workspace.

You must use a writable (mutable) workspace when creating Drill tables. For
example:

	"tmp": {
	      "location": "/tmp",
	      "writable": true,
	       }

## Complete CTAS Example

The following query returns one row from a JSON file that [you can download]({{site.baseurl}}/docs/sample-data-donuts):

	0: jdbc:drill:zk=local> SELECT id, type, name, ppu
	FROM dfs.`/Users/brumsby/drill/donuts.json`;
	+------------+------------+------------+------------+
	|     id     |    type    |    name    |    ppu     |
	+------------+------------+------------+------------+
	| 0001       | donut      | Cake       | 0.55       |
	+------------+------------+------------+------------+
	1 row selected (0.248 seconds)

To create and verify the contents of a table that contains this row:

  1. Set the workspace to a writable workspace.
  2. Set the `store.format` option appropriately.
  3. Run a CTAS statement that contains the query.
  4. Go to the directory where the table is stored and check the contents of the file.
  5. Run a query against the new table by querying the directory that contains the table.

The following sqlline output captures this sequence of steps.

### Workspace Definition

	"tmp": {
	      "location": "/tmp",
	      "writable": true,
	       }

### ALTER SESSION Command

    ALTER SESSION SET `store.format`='json';

### USE Command

	0: jdbc:drill:zk=local> USE dfs.tmp;
	+------------+------------+
	|     ok     |  summary   |
	+------------+------------+
	| true       | Default schema changed to 'dfs.tmp' |
	+------------+------------+
	1 row selected (0.03 seconds)

### CTAS Command

	0: jdbc:drill:zk=local> CREATE TABLE donuts_json AS
	SELECT id, type, name, ppu FROM dfs.`/Users/brumsby/drill/donuts.json`;
	+------------+---------------------------+
	|  Fragment  | Number of records written |
	+------------+---------------------------+
	| 0_0        | 1                         |
	+------------+---------------------------+
	1 row selected (0.107 seconds)

### File Contents

	administorsmbp7:tmp brumsby$ pwd
	/tmp
	administorsmbp7:tmp brumsby$ cd donuts_json
	administorsmbp7:donuts_json brumsby$ more 0_0_0.json
	{
	 "id" : "0001",
	  "type" : "donut",
	  "name" : "Cake",
	  "ppu" : 0.55
	}

### Query Against New Table

	0: jdbc:drill:zk=local> SELECT * FROM donuts_json;
	+------------+------------+------------+------------+
	|     id     |    type    |    name    |    ppu     |
	+------------+------------+------------+------------+
	| 0001       | donut      | Cake       | 0.55       |
	+------------+------------+------------+------------+
	1 row selected (0.053 seconds)

### Use a Different Output Format

You can run the same sequence again with a different storage format set for
the system or session (csv or Parquet). For example, if the format is set to
csv, and you name the directory donuts_csv, the resulting file would look like
this:

	administorsmbp7:tmp brumsby$ cd donuts_csv
	administorsmbp7:donuts_csv brumsby$ ls
	0_0_0.csv
	administorsmbp7:donuts_csv brumsby$ more 0_0_0.csv
	id,type,name,ppu
	0001,donut,Cake,0.55

