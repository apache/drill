---
title: "Data Sources and File Formats Introduction"
date: 2015-12-28 21:37:18 UTC
parent: "Data Sources and File Formats"
---
Included in the data sources that  Drill supports are these key data sources:

* HBase
* Hive
* MapR-DB
* File system

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
