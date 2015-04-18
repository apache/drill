---
title: "SHOW DATABASES AND SHOW SCHEMAS Command"
parent: "SQL Commands Summary"
---
The SHOW DATABASES and SHOW SCHEMAS commands generate a list of available Drill schemas that you can query.

## Syntax

The SHOW DATABASES and SHOW SCHEMAS commands support the following syntax:

    SHOW DATABASES;
    SHOW SCHEMAS;

{% include startnote.html %}These commands generate the same results.{% include endnote.html %}

## Usage Notes

You may want to run the SHOW DATABASES or SHOW SCHEMAS command to see a list of the configured storage plugins and workspaces in Drill before you issue the USE command to switch to a particular schema for your queries.

In Drill, a database or schema is a configured storage plugin instance or a configured storage plugin instance with a configured workspace. For example, dfs.donuts where dfs is the file system configured as a storage plugin instance, and donuts is a configured workspace.

You can configure and use multiple storage plugins and workspaces in Drill.  See [Storage Plugin Registration]({{ site.baseurl }}/docs/storage-plugin-registration) and [Workspaces]({{ site.baseurl }}/docs/workspaces).

## Example

The following example uses the SHOW DATABASES and SHOW SCHEMAS commands to generate a list of the available schemas in Drill. Some of the results that display are specific to all Drill installations, such as `cp.default` and `dfs.default`, while others vary based on your specific storage plugin and workspace configurations.

	0: jdbc:drill:zk=local> show databases;
	+-------------+
	| SCHEMA_NAME |
	+-------------+
	| dfs.default |
	| dfs.root  |
	| dfs.donuts  |
	| dfs.tmp   |
	| dfs.customers |
	| dfs.yelp  |
	| cp.default  |
	| sys       |
	| INFORMATION_SCHEMA |
	+-------------+
	9 rows selected (0.07 seconds)
	 
	 
	0: jdbc:drill:zk=local> show schemas;
	+-------------+
	| SCHEMA_NAME |
	+-------------+
	| dfs.default |
	| dfs.root  |
	| dfs.donuts  |
	| dfs.tmp   |
	| dfs.customers |
	| dfs.yelp  |
	| cp.default  |
	| sys       |
	| INFORMATION_SCHEMA |
	+-------------+
	9 rows selected (0.058 seconds)