---
title: "Query Data Introduction"
date: 2019-04-18
parent: "Query Data"
---
You can submit SQL queries against various data sources from the [Drill shell (SQLLine)]({{site.baseurl}}/docs/configuring-the-drill-shell/), [Drill Web UI]({{site.baseurl}}/docs/starting-the-web-ui/), [REST API]({{site.baseurl}}/docs/rest-api/), and tools that connect to Drill via [ODBC or JDBC]({{site.baseurl}}/docs/odbc-jdbc-interfaces/). Drill has [several storage and format plugins]({{site.baseurl}}/docs/connect-a-data-source-introduction/) that enable queries against multiple data sources and data formats, including [complex data]({{site.baseurl}}/docs/querying-complex-data). 
 
The following sections provide some general information about Drill queries.

## Specifying the Data Source Location
The optional [USE command]({{site.baseurl}}/docs/use) runs subsequent queries against a particular [storage plugin or schema]({{site.baseurl}}/docs/connect-a-data-source-introduction/). When you run the USE command to switch to a particular storage plugin or schema, you do not have to include the full path to the data in the FROM clause, for example:

The following query was run before switching to the dfs.schema. A workspace named "samples" was configured in the dfs storage plugin, creating a schema named `dfs.samples`. Notice that you have to use dot notation for the schema and back ticks around the table name. In some cases you may point to a directory or file in the schema, in which case you would put back ticks around the entire path, for example ```dfs.samples.`/nation/data/nation.parquet/````.  


	apache drill> select * from dfs.samples.`nation1`;
	+-------------+----------------+-------------+----------------------+
	| N_NATIONKEY |     N_NAME     | N_REGIONKEY |      N_COMMENT       |
	+-------------+----------------+-------------+----------------------+
	| 0           | ALGERIA        | 0           |  haggle. carefully f |
	| 1           | ARGENTINA      | 1           | al foxes promise sly |
	...
	+-------------+----------------+-------------+----------------------+

Running USE to switch to the `dfs.samples` schema: 

	apache drill> use dfs.samples;
	+------+-----------------------------------------+
	|  ok  |                 summary                 |
	+------+-----------------------------------------+
	| true | Default schema changed to [dfs.samples] |
	+------+-----------------------------------------+  

Query written without identifying the schema (without dot notation or back ticks):

	apache drill (dfs.samples)> select * from nation1;
	+-------------+----------------+-------------+----------------------+
	| N_NATIONKEY |     N_NAME     | N_REGIONKEY |      N_COMMENT       |
	+-------------+----------------+-------------+----------------------+
	| 0           | ALGERIA        | 0           |  haggle. carefully f |
	| 1           | ARGENTINA      | 1           | al foxes promise sly |
	...
	+-------------+----------------+-------------+----------------------+


## Casting Data
In some cases, Drill converts schema-less data to correctly-typed data implicitly. In this case, you do not need to [cast the data]({{site.baseurl}}/docs/supported-data-types/#casting-and-converting-data-types) to another type. The file format of the data and the nature of your query determines the requirement for casting or converting. Differences in casting depend on the data source. 

For example, you have to cast a string `"100"` in a JSON file to an integer in order to apply a math function
or an aggregate function.

To query HBase data using Drill, convert every column of an HBase table to/from byte arrays from/to an SQL data type as described in the section ["Querying HBase"]({{ site.baseurl}}/docs/querying-hbase/). Use [CONVERT_TO or CONVERT_FROM]({{ site.baseurl }}/docs//data-type-conversion/#convert_to-and-convert_from) functions to perform conversions of HBase data.

## Troubleshooting Queries

In addition to analyzing error messages printed by the Drill shell, you can troubleshoot queries from the [Profiles page]({{ site.baseurl }}/docs/identifying-performance-issues/) in the Drill Web UI or run the [EXPLAIN command]({{site.baseurl}}/docs/explain/) to review the query plan for issues. For example, if you run into a casting error, the query plan text may help you isolate the problem.

    0: jdbc:drill:zk=local> !set maxwidth 10000
    0: jdbc:drill:zk=local> explain plan for <query>;

[Drill shell commands]({{site.baseurl}}/docs/configuring-the-drill-shell/) include the `!set <set variable> <value>` to increase the default text display (number of characters). By default, most of the plan output is hidden.

## Query Syntax Tips

Remember the following tips when querying data with Drill:

  * Include a semicolon at the end of SQL statements, except when you issue a [Drill shell command]({{site.baseurl}}/docs/configuring-the-drill-shell/).   
    `Example: `!set maxwidth 10000`
  * Use backticks around [keywords]({{site.baseurl}}/docs/reserved-keywords), special characters, and [identifiers]({{site.baseurl}}/docs/lexical-structure/#identifier) that SQL cannot parse, such as the keyword default and a path that contains a forward slash character:
    Example: ``SELECT * FROM dfs.`default`.`/Users/drilluser/apache-drill-1.1.0/sample-data/nation.parquet`;``
  * When selecting all (SELECT *) schema-less data, the order of returned columns might differ from the stored order and might vary from query to query.  

###Syntax Highlighting and SQL Templates  
Drill 1.13 extends the syntax highlighting feature for storage plugin configurations to queries. 

You can see queries with highlighted syntax in the query profile on the Query tab, as shown in the following image:  

![](https://i.imgur.com/ZcXDQwV.png)  

The Edit Query tab auto-populates with the query so you can easily edit and rerun a query.  

In addition to syntax highlighting, an autocomplete feature enables you to use snippets (SQL templates) to quickly write syntax for queries.  
  
**Using the Autocomplete Feature**  

On the Query page in the Drill Web UI, place your cursor in the Query window and press `ctrl+space`. A drop-down menu of Drill keywords, functions, and templates appears. Use the up and down arrows on the keyboard to scroll through the list.  
 
**Note:** The `s*` option provides the template for a SELECT query.  

The following image shows the snippet for creating a temporary table:  

![](https://i.imgur.com/yKMSIRV.png)  

When you select the `ctas` option, the CTAS syntax is automatically written for you, as shown in the following image:  

![](https://i.imgur.com/TzmZFi5.png)


