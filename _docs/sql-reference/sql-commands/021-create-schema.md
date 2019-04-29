---
title: "CREATE OR REPLACE SCHEMA"
date: 2019-04-29
parent: "SQL Commands"
---

Starting in Drill 1.16, you can define a schema for text files using the CREATE OR REPLACE SCHEMA command. Running this command generates a hidden `.drill.schema` file in the table’s root directory. The `.drill.schema` file stores the schema definition in JSON format. Drill uses the schema file at runtime if the `exec.storage.enable_v3_text_reader` and `store.table.use_schema_file` options are enabled. Alternatively, you can create the schema file manually. If created manually, the file content must comply with the structure recognized by the Drill.  

##Syntax

The CREATE OR REPLACE SCHEMA command supports the following syntax:

	CREATE [OR REPLACE] SCHEMA
	[LOAD 'file:///path/to/file']
	[(column_name data_type nullability format default properties {prop='val', ...})]
	[FOR TABLE `table_name`]
	[PATH 'file:///schema_file_path/schema_file_name'] 
	[PROPERTIES ('key1'='value1', 'key2'='value2', ...)]  

##Parameters

*OR REPLACE*  
Existing schema is dropped and replaced with the new schema. Only supported when using FOR TABLE. Not supported when using PATH because it prevents malicious deletion of any file. You must manually delete any schema file created in a custom location. 

*LOAD*  
Loads raw schema (list of column names with their attributes) from a file. You must indicate the path to the file after the LOAD keyword. Note that columns should be listed or provided when using the LOAD clause; at least one option is required for the successful schema creation.

*column_name*  
Name of the column for which schema is created. Case-insensitive. 

*data_type*  
Data type defined for the column. See Supported Data Types. 

*format*  
Sets the format for date and time data types when converting from string.

*default*  
Sets a default value for non-nullable columns, such that queries return the default value instead of null. 

*properties*  
Keyword to include optional properties. See Related Options below.  
   
*property*      
Name of the property applied to the column or table.  
     
*value*      
Value set for the indicated property. 
 
*table_name*  
Name of the table associated with the schema being created or replaced. Enclose the table name in backticks if there are spaces after the FOR TABLE keywords. If the table does not exist, the command fails and schema is not created. If you indicate the table name without schema, the table is assumed to be in the current workspace, and you must specify the PATH property. If you indicate FOR TABLE and PATH, or you do not indicate either, the CREATE SCHEMA command fails. In this case, the table schema is created in the table’s root directory with the default name `.drill.table_schema`. Table cannot be a temporary table. 

*PATH*  
Path to the schema file. You must indicate the path to the file after the PATH keyword. 

*properties*  
List of properties as key-value pairs in  parenthesis.  

## Related Options 

You must enable the following options for Drill to use the schema created during query execution:
 
	set `exec.storage.enable_v3_text_reader` = true;
	+------+---------------------------------------------+
	|  ok  |                   summary                   |
	+------+---------------------------------------------+
	| true | exec.storage.enable_v3_text_reader updated. |
	+------+---------------------------------------------+
	
	set `store.table.use_schema_file` = true;
	+------+--------------------------------------+
	|  ok  |               summary                |
	+------+--------------------------------------+
	| true | store.table.use_schema_file updated. |
	+------+--------------------------------------+ 

## Related Properties  

When you create a schema, you can set the following properties within the CREATE [OR REPLACE] SCHEMA command:   

**drill.strict**  
A table property that determines the ordering of columns returned for wildcard (*) queries. Accepts a value of true or false. See Schema Mode (Column Order). 
 
**drill.format**  
A column property that ensures proper conversion when converting string values to date and time data types. See Format for Date, Time Conversion.

**drill.default**  
A column property that sets non-nullable columns to a “default” value when creating the schema. See Column Modes (Nullable and Non-Nullable Columns).  

**drill.blank-as**  
A property that sets how Drill handles blank column values. Accepts the following values:  
- **null**: If the column is nullable, treat the blank as null. If non-nullable, leave the blank unchanged.  
- **0**: Replace blanks with the value "0" for numeric types.   
- **skip**: Skip blank values. This sets the column to its default value: NULL for nullable columns, the default value for non-nullable columns.  
- If left empty, blanks have no special meaning. A blank is parsed as any other string, which typically produces an error.  

See Handling Policy for Blank Column Values.  

### Setting Properties
Include properties after the “properties” keyword, as shown in the following example where the date format is set to `'yyyy-MM-dd'` through the `drill.format` column property:

	create or replace schema (start_date date properties {'drill.format' = 'yyyy-MM-dd'}) for table dfs.tmp.`text_table`;
 
Alternatively, you can use “default” and “format” as keywords when creating schema, as shown in the following examples:
 
Setting the default for the non-nullable column “id” to -1 using the keyword “default”:
 
	create or replace schema (id int not null default '-1') for table dfs.tmp.`text_table`;

When you query the text_table, all blank values in the “id” column return a value of -1.  

###Storing Properties 
The defined schema and configured properties are stored and reflected in the schema file, `.drill.schema`, which you can see when you run DESCRIBE SCHEMA FOR TABLE. 

	describe schema for table dfs.tmp.`text_table`;
	+----------------------------------------------------------------------------------+
	|                            	      schema                                  	|
	+----------------------------------------------------------------------------------+
	| {
	  "table" : "dfs.tmp.`text_table`",
	  "schema" : {
	    "columns" : [
	  	{
	        "name" : "id",
	        "type" : "INT",
	        "mode" : "REQUIRED",
	        "properties" : {
	          "drill.default" : "-1"
	    	}
	  	},
	  	{
	        "name" : "start_date",
	        "type" : "DATE",
	        "mode" : "REQUIRED",
	        "properties" : {
	    	  "drill.format" : "yyyy-MM-dd",
	          "drill.default" : "2017-01-01"
	    	}
	  	}
		],
	    "properties" : {
	      "drill.strict" : "true"
		}
	  },
	  "version" : 1
	} |
	+----------------------------------------------------------------------------------+  


## Related Commands 


    DROP SCHEMA [IF EXISTS] FOR TABLE `table_name`
See Dropping Schema for a Table in the Examples section at the end of this topic. 


    DESCRIBE SCHEMA FOR TABLE `table_name`
See Describing Schema for a Table in the Examples section at the end of this topic.   

## Supported Data Types

Text files store information in string format and only support simple data types. You can use the CREATE [OR REPLACE] SCHEMA command to convert string data types in text files to the following data types:  
- INTEGER  
- BIGINT  
- DOUBLE  
- FLOAT  
- DECIMAL  
- BOOLEAN  
- VARCHAR  
- TIMESTAMP  
- DATE  
- TIME  
- INTERVAL [YEAR, MONTH, DAY, HOUR, MINUTE, SECOND]  

**Note:** Complex data types (arrays and maps) are not supported. 

Values are trimmed when converting to any type, except for varchar.  

## Usage Notes 

### General Information  
- Schema provisioning only works with tables defined as directories because Drill must have a place to store the schema file. The directory can contain one or more files.  
- Text files must have headers. The default extension for delimited text files with headers is `.csvh`. Note that the column names that appear in the headers match column definitions in the schema.  
- You do not have to enumerate all columns in a file when creating a schema. You can indicate the columns of interest only.  
- Columns in the defined schema do not have to be in the same order as in the data file. However, the names must match. The case can differ, for example “name” and “NAME” are acceptable.   
- Queries on columns with data types that cannot be converted fail with a `DATA_READ_ERROR`.   

### Schema Mode (Column Order)
The schema mode determines the ordering of columns returned for wildcard (*) queries. The mode is set through the `drill.strict` property. You can set this property to true (strict) or false (not strict). If you do not indicate the mode, the default is false (not strict).  

**Not Strict (Default)**  
Columns defined in the schema are projected in the defined order. Columns not defined in the schema are appended to the defined columns, as shown:  

	create or replace schema (id int, start_date date format 'yyyy-MM-dd') for table dfs.tmp.`text_table` properties ('drill.strict' = 'false');
	+------+-----------------------------------------+
	|  ok  |       	      summary             	|
	+------+-----------------------------------------+
	| true | Created schema for [dfs.tmp.text_table] |
	+------+-----------------------------------------+
	 
	select * from dfs.tmp.`text_table`;
	+------+------------+---------+
	|  id  | start_date |  name   |
	+------+------------+---------+
	| 1	| 2019-02-01 | Fred	|
	| 2	| 2018-11-30 | Wilma   |
	| 3	| 2016-01-01 | Pebbles |
	| 4	| null       | Barney  |
	| null | null   	| Dino	|
	+------+------------+---------+
 
Note that the “name” column, which was not included in the schema was appended to the end of the table.

**Strict**  
Setting the `drill.strict` property  to “true” changes the schema mode to strict, which means that the reader ignores any columns NOT included in the schema. The query only returns the columns defined in the schema, as shown:
 
	create or replace schema (id int, start_date date format 'yyyy-MM-dd') for table dfs.tmp.`text_table` properties ('drill.strict' = 'true');
	+------+-----------------------------------------+
	|  ok  |             	summary             	|
	+------+-----------------------------------------+
	| true | Created schema for [dfs.tmp.text_table] |
	+------+-----------------------------------------+
	 
	select * from dfs.tmp.`text_table`;
	+------+------------+
	|  id  | start_date |
	+------+------------+
	| 1	| 2019-02-01 |
	| 2	| 2018-11-30 |
	| 3	| 2016-01-01 |
	| 4	| null       |
	| null | null   	|
	+------+------------+  

Note that the “name” column, which was not included in the schema was ignored and not returned in the result set.  

## Including Additional Columns in the Schema
When you create a schema, you can include columns that do not exist in the table and these columns will be projected. This feature ensures that queries return the correct results whether the files have a specific column or not. Note that schema mode does not affect the behavior of this feature.
 
For example, the “comment” column is not in the text_table, but added when creating the schema:  

	create or replace schema (id int, start_date date format 'yyyy-MM-dd', comment varchar) for table dfs.tmp.`text_table`;
	+------+-----------------------------------------+
	|  ok  |             	summary             	 |
	+------+-----------------------------------------+
	| true | Created schema for [dfs.tmp.text_table] |
	+------+-----------------------------------------+

You can see the “comment” column returned in the result set.  

	select * from dfs.tmp.`text_table`;  
	+------+------------+---------+---------+
	|  id  | start_date | comment |  name   |
	+------+------------+---------+---------+
	| 1	   | 2019-02-01 |  null   | Fred	|
	| 2	   | 2018-11-30 |  null   | Wilma   |
	| 3	   | 2016-01-01 |  null   | Pebbles |
	| 4	   | null   	|  null   | Barney  |
	| null | null   	|  null   | Dino	|
	+------+------------+---------+---------+  

## Column Modes (Nullable and Non-Nullable Columns)
If a column in the schema is nullable (allows null values), and the column has a null value, the column value is returned as null. If the column is required (not nullable), but contains a null value, Drill returns the default value provided. If no default value is provided, Drill sets the column value to the natural default. 

For example, if you create a strict schema with two nullable columns (id and start_date), you can see that the missing values in both cases are null.

	create or replace schema (id int, start_date date format 'yyyy-MM-dd') for table dfs.tmp.`text_table` properties ('drill.strict' = 'true');
	+------+-----------------------------------------+
	|  ok  |             	summary             	|
	+------+-----------------------------------------+
	| true | Created schema for [dfs.tmp.text_table] |
	+------+-----------------------------------------+
	 
	select * from dfs.tmp.`text_table`;
	+------+------------+
	|  id  | start_date |
	+------+------------+
	| 1	| 2019-02-01 |
	| 2	| 2018-11-30 |
	| 3	| 2016-01-01 |
	| 4	| null   	|
	| null | null   	|
	+------+------------+
 
Updating the strict schema to have two required columns (id and start_date), you can see that the natural default was applied; 0 for id and 1970-01-01 for start_date.
 
	create or replace schema (id
	int not null, start_date date not null format 'yyyy-MM-dd') for table
	dfs.tmp.`text_table` properties ('drill.strict' = 'true');
	+------+-----------------------------------------+
	|  ok  |             	summary             	|
	+------+-----------------------------------------+
	| true | Created schema for [dfs.tmp.text_table] |
	+------+-----------------------------------------+
	 
	select * from dfs.tmp.`text_table`;
	+----+------------+
	| id | start_date |
	+----+------------+
	| 1  | 2019-02-01 |
	| 2  | 2018-11-30 |
	| 3  | 2016-01-01 |
	| 4  | 1970-01-01 |
	| 0  | 1970-01-01 |
	+----+------------+

Adding a default for each of these columns (-1 for id and 2017-01-01 for start_date),  you can see that the columns return the defined default value instead of the natural default.
 
	create or replace schema (id
	int not null default '-1', start_date date not null format 'yyyy-MM-dd' default
	'2017-01-01') for table dfs.tmp.`text_table` properties ('drill.strict' =
	'true');
	+------+-----------------------------------------+
	|  ok  |             	summary             	|
	+------+-----------------------------------------+
	| true | Created schema for [dfs.tmp.text_table] |
	+------+-----------------------------------------+
	 
	select * from dfs.tmp.`text_table`;
	+----+------------+
	| id | start_date |
	+----+------------+
	| 1  | 2019-02-01 |
	| 2  | 2018-11-30 |
	| 3  | 2016-01-01 |
	| 4  | 2017-01-01 |
	| -1 | 2017-01-01 |
	+----+------------+  

## Handling Policy for Blank Column Values
It is common for CSV files to have blank column values. The default
output for blank column values are empty strings (''), as shown:  

	select * from dfs.tmp.`text_blank`;
	+----+--------+------------+
	| id | amount | start_date |
	+----+--------+------------+
	| 1  | 20 	  | 2019-01-01 |
	| 2  |    	  |        	   |
	| 3  | 30 	  |            |
	+----+--------+------------+

When a schema is defined for columns, the default blank handling policy is `skip` which treats blank values as null, as shown:  

	create or replace schema (id
	int, amount double, start_date date format 'yyyy-MM-dd') for table
	dfs.tmp.`text_blank`;
	+------+-----------------------------------------+
	|  ok  |             	summary             	|
	+------+-----------------------------------------+
	| true | Created schema for [dfs.tmp.text_blank] |
	+------+-----------------------------------------+
	 
	select * from dfs.tmp.`text_blank`;
	+----+--------+------------+
	| id | amount | start_date |
	+----+--------+------------+
	| 1  | 20.0   | 2019-01-01 |
	| 2  | null   | null       |
	| 3  | 30.0   | null       |
	+----+--------+------------+

If a column is absent in the schema, the blank handling policy is default. Note that the blank handling policy is not applicable to varchar columns since they do not go through the type conversion logic.

You can configure how Drill handles blank column values through the `drill.blank-as` property when you create schema. 

In the following example, you can see the blank handling policy for the defined schema with the `drill.blank-as` property set to `0` on the “amount” column:
 
	create or replace schema (id int, amount double properties {'drill.blank-as' = '0'}, start_date date format 'yyyy-MM-dd') for table dfs.tmp.`text_blank`;
	+------+-----------------------------------------+
	|  ok  |             	summary             	|
	+------+-----------------------------------------+
	| true | Created schema for [dfs.tmp.text_blank] |
	+------+-----------------------------------------+
	 
	select * from dfs.tmp.`text_blank`;
	+----+--------+------------+
	| id | amount | start_date |
	+----+--------+------------+
	| 1  | 20.0   | 2019-01-01 |
	| 2  | 0.0	| null       |
	| 3  | 30.0   | null       |
	+----+--------+------------+  

## Format for Date, Time Conversion 
When you convert string values to date and time data types, include the format for proper conversion. 

You can include the format using the keyword “format,” as shown:  

	create or replace schema (start_date date format 'yyyy-MM-dd') for table dfs.tmp.`text_table`;

Alternatively, you can include the format in the column properties, as shown:  

	create or replace schema (start_date date properties {'drill.format' = 'yyyy-MM-dd'}) for table dfs.tmp.`text_table`;

Note that date, time type conversion uses the Joda time library, thus the format pattern must comply with the [Joda time supported format pattern](https://www.joda.org/joda-time/key_format.html). If the format is not indicated, ISO datetime formats are used:  

| **Type**  | **Accepted Format**                                                      |
|-----------|--------------------------------------------------------------------------|
| [Timestamp](https://www.joda.org/joda-time/apidocs/org/joda/time/format/ISODateTimeFormat.html#dateTimeNoMillis--) | yyyy-MM-dd'T'HH:mm:ssZZ                                                  |
| [Date](https://www.joda.org/joda-time/apidocs/org/joda/time/format/ISODateTimeFormat.html#localDateParser--)      | date-element = std-date-element |   ord-date-element | week-date-element |
|           | std-date-element  = yyyy ['-' MM ['-' dd]]                               |
|           | ord-date-element  = yyyy ['-' DDD]                                       |
|           | week-date-element = xxxx '-W' ww ['-' e]                                 |
| [Time](https://www.joda.org/joda-time/apidocs/org/joda/time/format/ISODateTimeFormat.html#localTimeParser--)      | time = ['T'] time-element                                                |
|           | time-element = HH [minute-element] | [fraction]                          |
|           | minute-element = ':' mm [second-element] |   [fraction]                  |
|           | second-element = ':' ss [fraction]                                       |
|           | fraction       =   ('.' | ',') digit+                                    |


## Limitations
None

## Examples
Examples throughout this topic use the files and directories described in the following section, Directory and File Setup.   

###Directory and File Setup

	[root@doc23 text_table]# pwd
	/tmp/text_table
	[root@doc23 text_table]# ls
	1.csvh  2.csvh
	
	[root@doc23 text_table]# cat 1.csvh
	id,name,start_date
	1,Fred,2019-02-01
	2,Wilma,2018-11-30
	3,Pebbles,2016-01-01
	4,Barney
	
	[root@doc23 text_table]# cat 2.csvh
	name
	Dino
	
	[root@doc23 tmp]# cd text_blank/
	[root@doc23 text_blank]# ls
	blank.csvh
	
	[root@doc23 text_blank]# cat blank.csvh
	id,amount,start_date
	1,20,2019-01-01
	2,,
	3,30,


Query the directory text_table. 

	select * from dfs.tmp.`text_table`;
	+----+---------+------------+
	| id |  name   | start_date |
	+----+---------+------------+
	| 1  | Fred    | 2019-02-01 |
	| 2  | Wilma   | 2018-11-30 |
	| 3  | Pebbles | 2016-01-01 |
	| 4  | Barney  |            |
	|    | Dino    |            |
	+----+---------+------------+

Notice that the query plan contains a scan and project:
	
	| 00-00    Screen
	00-01      Project(**=[$0])
	00-02        Scan(table=[[dfs, tmp, text_table]], groupscan=[EasyGroupScan [selectionRoot=file:/tmp/text_table, numFiles=2, columns=[`**`], files=[file:/tmp/text_table/1.csvh, file:/tmp/text_table/2.csvh], schema=null]])
	
Using the sqltypeof and modeof functions, you can see that each column is defined as a non-nullable string (varchar), and missing columns are defined as empty strings:  
 
	select sqltypeof(id), modeof(id) from dfs.tmp.`text_table` limit 1;
	+-------------------+----------+
	|      EXPR$0       |  EXPR$1  |
	+-------------------+----------+
	| CHARACTER VARYING | NOT NULL |
	+-------------------+----------+  

### Creating a Schema
Create a defined schema for the text_table directory. When you define schema, you do not have to enumerate all columns. The columns in the defined schema are not required to be in the same order as the data file. Note that the name of the columns must match, but can differ in case.

Define schema for the id column:
	 
	create schema (id int) for table dfs.tmp.`text_table`;
	+------+-----------------------------------------+
	|  ok  |                 summary                 |
	+------+-----------------------------------------+
	| true | Created schema for [dfs.tmp.text_table] |
	+------+-----------------------------------------+  

After you define a schema, you can see the schema file (stored in JSON format) in the root table directory: 

	 [root@doc23 text_table]# ls -a
	.  ..  1.csvh  2.csvh  .drill.schema  ..drill.schema.crc  
	
	[root@doc23 text_table]# cat .drill.schema
	{
	  "table" : "dfs.tmp.`text_table`",
	  "schema" : {
	    "columns" : [
	      {
	        "name" : "id",
	        "type" : "INT",
	        "mode" : "OPTIONAL"
	      }
	    ]
	  },
	  "version" : 1

Query the text_table directory to see how the schema is applied:

	select * from dfs.tmp.`text_table`;
	+------+---------+------------+
	|  id  |  name   | start_date |
	+------+---------+------------+
	| 1    | Fred    | 2019-02-01 |
	| 2    | Wilma   | 2018-11-30 |
	| 3    | Pebbles | 2016-01-01 |
	| 4    | Barney  |            |
	| null | Dino    |            |
	+------+---------+------------+  

After defining the schema on the id column, you can see that the `id` column type and mode is defined as a nullable integer, while other columns types were inferred as non-nullable VarChar:

	select sqltypeof(id), modeof(id) from dfs.tmp.`text_table` limit 1;
	+---------+----------+
	| EXPR$0  |  EXPR$1  |
	+---------+----------+
	| INTEGER | NULLABLE |
	+---------+----------+

Running EXPLAIN PLAN, you can see that type conversion was done while reading data from source; no additional plan stages were added:

	explain plan for select * from dfs.tmp.`text_table`;
	| 00-00    Screen
	00-01      Project(**=[$0])
	00-02        Scan(table=[[dfs, tmp, text_table]], groupscan=[EasyGroupScan [selectionRoot=file:/tmp/text_table, numFiles=2, columns=[`**`], files=[file:/tmp/text_table/1.csvh, file:/tmp/text_table/2.csvh], schema=[TupleSchema [PrimitiveColumnMetadata [`id` (INT(0, 0):OPTIONAL)]]]]])  

### Describing Schema for a Table
After you create schema, you can examine the schema using the DESCRIBE SCHEMA FOR TABLE command. Schema can print to JSON or STATEMENT format. JSON format is the default if no format is indicated in the query. Schema displayed in JSON format is the same as the JSON format in the `.drill.schema` file.

	describe schema for table dfs.tmp.`text_table` as JSON;
	+----------------------------------------------------------------------------------+
	|                                      schema                                      |
	+----------------------------------------------------------------------------------+
	| {
	  "table" : "dfs.tmp.`text_table`",
	  "schema" : {
	    "columns" : [
	      {
	        "name" : "id",
	        "type" : "INT",
	        "mode" : "OPTIONAL"
	      }
	    ]
	  },
	  "version" : 1
	} |
	+----------------------------------------------------------------------------------+

STATEMENT format displays the schema in a form compatible with the CREATE OR REPLACE SCHEMA command such that it can easily be copied, modified, and executed.

	describe schema for table dfs.tmp.`text_table` as statement;
	+--------------------------------------------------------------------------+
	|                                  schema                                  |
	+--------------------------------------------------------------------------+
	| CREATE OR REPLACE SCHEMA
	(
	`id` INT
	)
	FOR TABLE dfs.tmp.`text_table`
	 |
	+--------------------------------------------------------------------------+

### Dropping Schema for a Table
You can easily drop the schema for a table using the DROP SCHEMA [IF EXISTS] FOR TABLE \`table_name` command, as shown:

	use dfs.tmp;
	+------+-------------------------------------+
	|  ok  |               summary               |
	+------+-------------------------------------+
	| true | Default schema changed to [dfs.tmp] |
	+------+-------------------------------------+
	
	drop schema for table `text_table`;
	+------+---------------------------------------+
	|  ok  |                summary                |
	+------+---------------------------------------+
	| true | Dropped schema for table [text_table] |
	+------+---------------------------------------+
	

##Troubleshooting 

**Schema defined as incorrect data type produces DATA_READ_ERROR**  
Assume that you defined schema on the “name” column as integer, as shown:
	create or replace schema (name int) for table dfs.tmp.`text_table`;
	+------+-----------------------------------------+
	|  ok  |                 summary                 |
	+------+-----------------------------------------+
	| true | Created schema for [dfs.tmp.text_table] |
	+------+-----------------------------------------+

Because the column does not contain integers, a query issued against the directory returns the DATA_READ_ERROR. The error message includes the line and value causing the issue:  

	select * from dfs.tmp.`text_table`;
	
	Error: DATA_READ ERROR: Illegal conversion: Column `name` of type INT, Illegal value `Fred`
	
	Line 1
	Record 0
	Fragment 0:0
	
	[Error Id: db6afe96-1014-4a98-a034-10e36daa1aa7 on doc23.lab:31010] (state=,code=0)
	
	Incorrect syntax
	Required syntax properties are missing or incorrectly specified. Check CREATE SCHEMA syntax to ensure that all required properties where specified.

**Missing backticks**  
Unable to parse CREATE SCHEMA command due to unknown properties and keywords. If the specified table name or column name contains spaces, enclose the name in backticks.

**Incorrect data type**  
Parsing has failed on unknown/unsupported data type. Check which data types are supported by Drill. Check syntax for complex data types.  

**Missing table name**  
Table name indicated after `FOR TABLE` keywords are not present. Check if the table exists in the specified schema or in the current schema (if the schema was not specified). If you do not want to bind schema to the specific table, use the PATH property.

**Existing schema file**  
A schema file already exists for the table. Use the DROP SCHEMA command to remove stale or mistakenly created schema file. Alternatively, you can use the CREATE OR REPLACE SCHEMA command.

**Lack of permissions**  
Unable to create schema file in the directory. Verify that you have write permission on the table’s root directory or the directory specified. If not, indicate the directory on which you write permissions in the command syntax.







 







  







