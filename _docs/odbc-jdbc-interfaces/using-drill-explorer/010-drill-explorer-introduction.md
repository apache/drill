---
title: "Drill Explorer Introduction"
date: 2018-11-02
parent: "Using Drill Explorer"
---

Drill Explorer is a user interface for browsing Drill data
sources, previewing the results of a SQL query, and creating a view. Typically, you use Drill Explorer to explore data or to
create a view that you can query as if it were a table. For example, before designing a report using a BI reporting tool, use Drill Explorer to quickly familiarize yourself with the data. In an ODBC-compliant BI tool, use the ODBC DSN to create an ODBC connection. 

You can connect Drill to a Hive data source, use Drill Explorer to create a view, and connect to the view as if it were a table. Similarly, you can connect Drill to HBase, Parquet, JSON, CSV, or TSV files, create a view, and query the view as if it were a table.

{% include startnote.html %}The default schema that you configure in the DSN may or may not carry over to an application’s data source connections. You may need to re-select the schema.{% include endnote.html %}


