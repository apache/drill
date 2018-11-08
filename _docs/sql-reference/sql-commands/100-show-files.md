---
title: "SHOW FILES"
date: 2018-11-08
parent: "SQL Commands"
---
The SHOW FILES command provides a quick report of the file systems that are
visible to Drill for query purposes. This command is unique to Apache Drill. Starting in Drill 1.15, the INFORMATION_SCHEMA includes a FILES table that you can query. See [Querying the INFORMATION_SCHEMA]({{site.baseurl}}/docs/querying-the-information-schema/). 

## Syntax

The SHOW FILES command supports the following syntax.

    SHOW FILES [ FROM filesystem.directory_name | IN filesystem.directory_name ];

The FROM or IN clause is required if you do not specify a default file system
first. You can do this with the USE command. FROM and IN are synonyms.

The directory name is optional. (If the directory name is a Drill reserved
word, you must use back ticks around the name.)

The command returns standard Linux `stat` information for each file or
directory, such as permissions, owner, and group values. This information is
not specific to Drill.

## Examples

The following example returns information about directories and files in the
local (`dfs`) file system.

	0: jdbc:drill:> use dfs;
	 
	+------------+------------+
	|     ok     |  summary   |
	+------------+------------+
	| true       | Default schema changed to 'dfs' |
	+------------+------------+
	1 row selected (0.318 seconds)
	 
	0: jdbc:drill:> show files;
	+------------+-------------+------------+------------+------------+------------+-------------+------------+------------------+
	|    name    | isDirectory |   isFile   |   length   |   owner    |   group    | permissions | accessTime | modificationTime |
	+------------+-------------+------------+------------+------------+------------+-------------+------------+------------------+
	| user       | true        | false      | 1          | abcd       | abcd       | rwxr-xr-x   | 2014-07-30 21:37:06.0 | 2014-07-31 22:15:53.193 |
	| backup.tgz | false       | true       | 36272      | root       | root       | rw-r--r--   | 2014-07-31 22:09:13.0 | 2014-07-31 22:09:13.211 |
	| JSON       | true        | false      | 1          | root       | root       | rwxr-xr-x   | 2014-07-31 15:22:42.0 | 2014-08-04 15:43:07.083 |
	| scripts    | true        | false      | 3          | root       | root       | rwxr-xr-x   | 2014-07-31 22:10:51.0 | 2014-08-04 18:23:09.236 |
	| temp       | true        | false      | 2          | root       | root       | rwxr-xr-x   | 2014-08-01 20:07:37.0 | 2014-08-01 20:09:42.595 |
	| hbase      | true        | false      | 10         | abcd       | abcd       | rwxr-xr-x   | 2014-07-30 21:36:08.0 | 2014-08-04 18:31:13.778 |
	| tables     | true        | false      | 0          | root       | root       | rwxrwxrwx   | 2014-07-31 22:14:35.0 | 2014-08-04 15:42:43.415 |
	| CSV        | true        | false      | 4          | root       | root       | rwxrwxrwx   | 2014-07-31 17:34:53.0 | 2014-08-04
	...

The following example shows the files in a specific directory in the `dfs`
file system:

	0: jdbc:drill:> show files in dfs.CSV;
	 
	+------------+-------------+------------+------------+------------+------------+-------------+------------+------------------+
	|    name    | isDirectory |   isFile   |   length   |   owner    |   group    | permissions | accessTime | modificationTime |
	+------------+-------------+------------+------------+------------+------------+-------------+------------+------------------+
	| customers.csv | false       | true       | 62011      | root       | root       | rw-r--r--   | 2014-08-04 18:30:39.0 | 2014-08-04 18:30:39.314 |
	| products.csv.small | false       | true       | 34972      | root       | root       | rw-r--r--   | 2014-07-31 23:58:42.0 | 2014-07-31 23:59:16.849 |
	| products.csv | false       | true       | 34972      | root       | root       | rw-r--r--   | 2014-08-01 06:39:34.0 | 2014-08-04 15:58:09.325 |
	| products.csv.bad | false       | true       | 62307      | root       | root       | rw-r--r--   | 2014-08-04 15:58:02.0 | 2014-08-04 15:58:02.612 |
	+------------+-------------+------------+------------+------------+------------+-------------+------------+------------------+
	4 rows selected (0.165 seconds)
