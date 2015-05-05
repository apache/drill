---
title: "Connecting to ODBC Data Sources"
parent: "Using ODBC on Windows"
---
After you create the ODBC DSN, you can use ODBC on Windows to directly connect to data
that is defined by a schema, such as Hive, and data that is self-describing.
Examples of self-describing data include HBase, Parquet, JSON, CSV,and TSV.

In some cases, you may want to use Drill Explorer to explore that data or to
create a view before you connect to the data from a BI tool. For more
information about Drill Explorer, see [Using Drill Explorer on Windows]({{ site.baseurl }}/docs/using-drill-explorer-on-windows) to browse data and create views.

In an ODBC-compliant BI tool, use the ODBC DSN to create an ODBC connection
with one of the methods applicable to the data source type:

| Data Source Type               | ODBC Connection Method                                                                                                                                      |
|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Hive                           | Connect to a table. Connect to the table using custom SQL. Use Drill Explorer to create a view. Then use ODBC to connect to the view as if it were a table. |
| HBase, Parquet, JSON, CSV, TSV | Use Drill Explorer to create a view. Then use ODBC to connect to the view as if it were a table. Connect to the data using custom SQL.                      |
  
{% include startnote.html %}The default schema that you configure in the DSN may or may not carry over to an application’s data source connections. You may need to re-select the schema.{% include endnote.html %}


