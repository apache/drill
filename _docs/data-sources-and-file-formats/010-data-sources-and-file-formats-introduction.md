---
title: "Data Sources and File Formats Introduction"
parent: "Data Sources and File Formats"
---
Included in the data sources that  Drill supports are these key data sources:

* HBase
* Hive
* MapR-DB
* File system

Drill supports the following input formats for data:

* CSV (Comma-Separated-Values)
* TSV (Tab-Separated-Values)
* PSV (Pipe-Separated-Values)
* Parquet
* JSON

You set the input format for data coming from data sources to Drill in the workspace portion of the [storage plugin](/docs/storage-plugin-registration) definition. The default input format in Drill is Parquet. 

You change the [sys.options table](/docs/planning-and-execution-options) to set the output format of Drill data. The default storage format for Drill Create Table AS (CTAS) statements is Parquet.