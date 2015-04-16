
---
title: "ODBC on Windows Introduction"
parent: "Using ODBC on Windows"
---
The MapR Drill ODBC driver provides BI tools access to Drill’s flexible query
capabilities so that users can quickly explore various data sources. The MapR
Drill ODBC driver includes Drill Explorer, which is a simple user interface
that enables users to examine the content of data sources and create views
before visualizing the data in a BI tool.

Once you install the MapR Drill ODBC Driver, you can create ODBC DSNs to Drill
data sources using the ODBC Administrator tool and then use the DSNs to access
the data from BI tools that work with ODBC. Drill can connect to data with
well-defined schemas, such as Hive. Drill can also connect directly to data
that is self-describing, such as HBase, Parquet, JSON, CSV, and TSV.

Complete the following steps to connect to a Drill data source from a BI tool
using ODBC:

  * [Step 1. Install the MapR Drill ODBC Driver]({{ site.baseurl }}/docs/step-1-install-the-mapr-drill-odbc-driver-on-windows)
  * [Step 2. Configure ODBC Connections to Drill Data Sources]({{ site.baseurl }}/docs/step-2-configure-odbc-connections-to-drill-data-sources)
  * [Step 3. Connect to Drill Data Sources from a BI Tool]({{ site.baseurl }}/docs/step-3-connect-to-drill-data-sources-from-a-bi-tool)

For examples of how you can use the MapR Drill ODBC Driver to connect to Drill
Data Sources from BI tools, see [Step 3. Connect to Drill Data Sources from a
BI Tool]({{ site.baseurl }}/docs/step-3-connect-to-drill-data-sources-from-a-bi-tool). While the documentation includes examples for Tableau, you can use
this driver with any BI tool that works with ODBC, such as Excel,
MicroStrategy, and Toad.

The following figure shows how a BI tool on Windows uses an ODBC connection to
access data from a Hive table:

![BI to Drill Interface]({{ site.baseurl }}/docs/img/BI_to_Drill_2.png)

The following components provide applications access to Drill data sources:

<table ><tbody><tr><th >Component</th><th >Role</th></tr><tr><td valign="top">Drillbit</td><td valign="top">Accepts queries from clients, executes queries against Drill data sources, and returns the query results. </td></tr><tr><td valign="top">ODBC Data Source Administrator</td><td valign="top">The ODBC Data Source Administrator enables the creation of DSNs to Apache Drill data sources.<br /> In the figure above, the ODBC Data Source Administrator was used to create <code>Hive-DrillDataSources</code>.</td></tr><tr><td valign="top">ODBC DSN</td><td valign="top"><p>Provides applications information about how to connect to the Drill Source.</p>In the figure above, the <code>Hive-DrillDataSources</code> is a DSN that provides connection information to the Hive tables.</td></tr><tr><td colspan="1" valign="top">BI Tool</td><td colspan="1" valign="top"><p>Accesses Drill data sources using the connection information from the ODBC DSN.</p>In the figure above, the BI tool uses <code>Hive-DrillDataSources</code> to access the <code>hive_student</code> table.</td></tr></tbody></table></div>