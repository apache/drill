---
title: "Data Sources and File Formats Introduction"
date: 2016-05-05 21:44:33 UTC
parent: "Data Sources and File Formats"
---
Included in the data sources that  Drill supports are these key data sources:

* HBase
* Hive
* MapR-DB
* File system  

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

You change one of the `store` property in the [sys.options table]({{ site.baseurl }}/docs/configuration-options-introduction/) to set the output format of Drill data. The default storage format for Drill CREATE TABLE AS (CTAS) statements is Parquet.

