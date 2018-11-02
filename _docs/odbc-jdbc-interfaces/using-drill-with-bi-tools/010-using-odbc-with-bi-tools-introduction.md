---
title: "Using Drill with BI Tools Introduction"
date: 2018-11-02
parent: "Using Drill with BI Tools"
---
The MapR Drill ODBC driver provides BI tools access to Drill’s flexible query
capabilities so that users can quickly explore various data sources. The MapR
Drill ODBC driver includes Drill Explorer, a simple user interface
for examining the content of data sources and creating views
before visualizing the data in a BI tool.

After you install the MapR Drill ODBC Driver, you can create ODBC DSNs to Drill
data sources, and then use the DSNs to access
the data from BI tools that work with ODBC. Drill can connect to data with
well-defined schemas, such as Hive. Drill can also connect directly to data
that is self-describing, such as HBase, Parquet, JSON, CSV, and TSV.

The following figure shows how a BI tool on Windows uses an ODBC connection to
access data from a Hive table:

![BI to Drill Interface]({{ site.baseurl }}/docs/img/BI_to_Drill_2.png)

The following components provide applications access to Drill data sources:

| Component                      | Role                                                                                                                                                                                            |
|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Drillbit                       | Accepts queries from clients, executes queries against Drill data sources, and returns the query results.                                                                                       |
| ODBC Data Source Administrator | The ODBC Data Source Administrator enables the creation of DSNs to Apache Drill data sources. In the figure above, the ODBC Data Source Administrator was used to create Hive-DrillDataSources. |
| ODBC DSN                       | Provides applications information about how to connect to the Drill Source.In the figure above, the Hive-DrillDataSources is a DSN that provides connection information to the Hive tables.     |
| BI Tool                        | Accesses Drill data sources using the connection information from the ODBC DSN. In the figure above, the BI tool uses Hive-DrillDataSources to access the hive_student table.                   |
