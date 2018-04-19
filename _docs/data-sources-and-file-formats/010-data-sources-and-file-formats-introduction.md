---
title: "Data Sources and File Formats Introduction"
date: 2018-04-19 01:45:23 UTC
parent: "Data Sources and File Formats"
---
Drill supports the following key data sources:

* HBase
* Hive
* MapR-DB
* File system 
* ...  

See [Connect a Data Source]({{site.baseurl}}/docs/connect-a-data-source/) for a complete list of supported data sources that you can configure in Drill. 

Drill considers data sources to have either a strong schema or a weak schema.  

The following table describes each schema type:

| Schema Type | Description                                                                                                                                                                                                                                                                                                                                                           |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Strong      | With the exception of text file data sources, Drill verifies that data sources associated with a strong schema contain data types compatible with those used in the query. Drill also verifies that the columns referenced in the query exist in the underlying data sources. If the columns do not exist, CREATE VIEW fails.                                         |
| Weak        | Drill does not verify that data sources associated with a weak schema contain data types compatible with those used in the query. Drill does not verify if columns referenced in a query on a Parquet data source exist, therefore CREATE VIEW always succeeds. In the case of JSON files, Drill does not verify if the files contain the maps specified in the view. |

The following table lists the current categories of schema and the data
sources associated with each:

|              | Strong Schema                                   | Weak Schema                                     |
|--------------|-------------------------------------------------|-------------------------------------------------|
| Data Sources | views, hive tables, hbase column families, text | json, mongodb, hbase column qualifiers, parquet |


Drill supports the following input formats for data:

* [Avro](http://avro.apache.org/docs/current/spec.html)
* CSV (Comma-Separated-Values)
* TSV (Tab-Separated-Values)
* PSV (Pipe-Separated-Values)
* Parquet
* MapR-DB*
* Hadoop Sequence Files

\* Only available when you install Drill on a cluster using the mapr-drill package.

You set the input format for data coming from data sources to Drill in the workspace portion of the [storage plugin]({{ site.baseurl }}/docs/storage-plugin-registration) definition. The default input format in Drill is Parquet. 

You change one of the `store` properties in the [sys.options table]({{ site.baseurl }}/docs/configuration-options-introduction/) to set the output format of Drill data. The default storage format for Drill CREATE TABLE AS (CTAS) statements is Parquet.  

##Schemaless Tables  
As of Drill 1.13, Drill supports queries on empty directories. Empty directories are directories that exist, but do not contain files. Currently, an empty directory in Drill is a Drill table without a schema, or a “schemaless” table. An empty directory with Parquet metadata cache files is also a schemaless table in Drill.
 
Drill supports queries with JOIN and UNION [ALL] operators on empty directories. For example, if you issue the following queries with the UNION ALL operator, Drill queries the empty directory (empty_DIR) as a schemaless table and returns results for the query on the right side of the operator:  

       0: jdbc:drill:schema=dfs.tmp> select columns[0] from empty_DIR UNION ALL select cast(columns[0] as int) c1 from `testWindow.csv`;  

###Usage Notes  

- Queries with stars (*) on an empty directory return an empty result set.  
- Fields indicated in the SELECT statement are returned as INT-OPTIONAL types.  
- An empty directory in a query does not change the results; Drill returns results as if the query does not contain the UNION operator.  
- You can use an empty directory in complex queries.  
- Queries with joins return an empty result, except when using outer join clauses, when the outer table for "right join" or derived table for "left join" has data. In that case, Drill returns the data from the table with data.




