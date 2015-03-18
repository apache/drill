---
title: "Step 3. Connect to Drill Data Sources from a BI Tool"
parent: "Using the MapR ODBC Driver on Windows"
---
[Previous](/docs/step-2-configure-odbc-connections-to-drill-data-sources)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Back to Table of Contents](/docs)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Next](/docs/tableau-examples)

After you create the ODBC DSN, you can use ODBC to directly connect to data
that is defined by a schema, such as Hive, and data that is self-describing.
Examples of self-describing data include HBase, Parquet, JSON, CSV,and TSV.

In some cases, you may want to use Drill Explorer to explore that data or to
create a view before you connect to the data from a BI tool. For more
information about Drill Explorer, see [Using Drill Explorer to Browse Data and
Create Views](/docs/using-drill-explorer-to-browse-data-and-create-views).

In an ODBC-compliant BI tool, use the ODBC DSN to create an ODBC connection
with one of the methods applicable to the data source type:

<table ><tbody><tr><th >Data Source Type</th><th >ODBC Connection Method</th></tr><tr><td valign="top">Hive</td><td valign="top">Connect to a table.<br />Connect to the table using custom SQL.<br />Use Drill Explorer to create a view. Then use ODBC to connect to the view as if it were a table.</td></tr><tr><td valign="top">HBase<br /><span style="line-height: 1.4285715;background-color: transparent;">Parquet<br /></span><span style="line-height: 1.4285715;background-color: transparent;">JSON<br /></span><span style="line-height: 1.4285715;background-color: transparent;">CSV<br /></span><span style="line-height: 1.4285715;background-color: transparent;">TSV</span></td><td valign="top">Use Drill Explorer to create a view. Then use ODBC to connect to the view as if it were a table.<br />Connect to the data using custom SQL.</td></tr></tbody></table>
  
For examples of how to connect to Drill data sources from a BI tool, see the
[Step 3. Connect to Drill Data Sources from a BI Tool](/docs/step-3-connect-to-drill-data-sources-from-a-bi-tool).

**Note:** The default schema that you configure in the DSN may or may not carry over to an application’s data source connections. You may need to re-select the schema.

