---
title: "Drill Explorer Introduction"
parent: "Using Drill Explorer"
---

Drill Explorer is a user interface for browsing Drill data
sources, previewing the results of a SQL query, and creating a view. Typically, you use Drill Explorer to explore data or to
create a view before you connect to the data from a BI tool. For example, before designing a report using a BI reporting tool, use Drill Explorer to familiarize yourself with the data. In an ODBC-compliant BI tool, use the ODBC DSN to create an ODBC connection
with one of the methods applicable to the data source type. 

To connect Drill to a Hive data source, follow these steps:

1. Connect to a Hive table.
2. Use Drill Explorer to create a view.
3. Connect to the view as if it were a table.

To connect Drill to an HBase, Parquet, JSON, CSV, or TSV, follow these steps:

1. Use Drill Explorer to create a view.
2. Use ODBC to connect to the view as if it were a table.
3. Use Drill Explorer to query the table.
 
{% include startnote.html %}The default schema that you configure in the DSN may or may not carry over to an application’s data source connections. You may need to re-select the schema.{% include endnote.html %}


