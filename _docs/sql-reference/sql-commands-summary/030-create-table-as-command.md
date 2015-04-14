---
title: "CREATE TABLE AS (CTAS) command"
parent: "SQL Commands Summary"
---
You can create tables in Drill by using the CTAS command:

    CREATE TABLE new_table_name AS <query>;

where query is any valid Drill query. Each table you create must have a unique
name. You can include an optional column list for the new table. For example:

    create table logtable(transid, prodid) as select transaction_id, product_id from ...

You can store table data in one of three formats:

  * csv
  * parquet
  * json

The parquet and json formats can be used to store complex data.

To set the output format for a Drill table, set the `store.format` option with
the ALTER SYSTEM or ALTER SESSION command. For example:

    alter session set `store.format`='json';

Table data is stored in the location specified by the workspace that is in use
when you run the CTAS statement. By default, a directory is created, using the
exact table name specified in the CTAS statement. A .json, .csv, or .parquet
file inside that directory contains the data.

You can only create new tables in workspaces. You cannot create tables in
other storage plugins such as Hive and HBase.

You must use a writable (mutable) workspace when creating Drill tables. For
example:

	"tmp": {
	      "location": "/tmp",
	      "writable": true,
	       }

## Example

The following query returns one row from a JSON file:

	0: jdbc:drill:zk=local> select id, type, name, ppu
	from dfs.`/Users/brumsby/drill/donuts.json`;
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
  5. Run a query against the new table.

The following sqlline output captures this sequence of steps.

### Workspace Definition

	"tmp": {
	      "location": "/tmp",
	      "writable": true,
	       }

### ALTER SESSION Command

    alter session set `store.format`='json';

### USE Command

	0: jdbc:drill:zk=local> use dfs.tmp;
	+------------+------------+
	|     ok     |  summary   |
	+------------+------------+
	| true       | Default schema changed to 'dfs.tmp' |
	+------------+------------+
	1 row selected (0.03 seconds)

### CTAS Command

	0: jdbc:drill:zk=local> create table donuts_json as
	select id, type, name, ppu from dfs.`/Users/brumsby/drill/donuts.json`;
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

	0: jdbc:drill:zk=local> select * from donuts_json;
	+------------+------------+------------+------------+
	|     id     |    type    |    name    |    ppu     |
	+------------+------------+------------+------------+
	| 0001       | donut      | Cake       | 0.55       |
	+------------+------------+------------+------------+
	1 row selected (0.053 seconds)

### Use a Different Output Format

You can run the same sequence again with a different storage format set for
the system or session (csv or parquet). For example, if the format is set to
csv, and you name the table donuts_csv, the resulting file would look like
this:

	administorsmbp7:tmp brumsby$ cd donuts_csv
	administorsmbp7:donuts_csv brumsby$ ls
	0_0_0.csv
	administorsmbp7:donuts_csv brumsby$ more 0_0_0.csv
	id,type,name,ppu
	0001,donut,Cake,0.55

