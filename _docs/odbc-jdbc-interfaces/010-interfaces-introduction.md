---
title: "Interfaces Introduction"
date: 2017-08-18 17:47:49 UTC
parent: "ODBC/JDBC Interfaces"
---
You can connect to Apache Drill through the following interfaces:

  * Drill shell
  * Drill Web Console
  * [ODBC]({{ site.baseurl }}/docs/installing-the-odbc-driver/)*
  * [JDBC]({{ site.baseurl }}/docs/using-jdbc-with-squirrel-on-windows/)
  * C++ API

*Apache Drill does not have an open source ODBC driver. However, MapR provides an ODBC driver developed specifically for connecting Apache Drill to BI tools. 

## Using ODBC to Access Apache Drill from BI Tools

MapR provides an ODBC driver that connects Windows, Mac OS X, and Linux to Apache Drill and BI tools. Install the latest version of Apache Drill with the latest version of the MapR Drill ODBC driver. 

Access the latest MapR Drill ODBC drivers at [http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/).

## Using JDBC to Access Apache Drill from SQuirrel

You can connect to Drill through a JDBC client tool, such as SQuirreL, on
Windows, Linux, and Mac OS X systems, to access all of your data sources
registered with Drill. An embedded JDBC driver is included with Drill.
Configure the JDBC driver in the SQuirreL client to connect to Drill from
SQuirreL. This section provides instruction for connecting to Drill from
SQuirreL on Windows.

To use the Drill JDBC driver with SQuirreL on Windows, complete the following
steps:

  * [Step 1: Getting the Drill JDBC Driver]({{ site.baseurl }}/docs/using-the-jdbc-driver/#getting-the-drill-jdbc-driver) 
  * [Step 2: Installing and Starting SQuirreL]({{ site.baseurl }}/docs/using-jdbc-with-squirrel-on-windows/#step-2:-installing-and-starting-squirrel)
  * [Step 3: Adding the Drill JDBC Driver to SQuirreL]({{ site.baseurl }}/docs/using-jdbc-with-squirrel-on-windows/#step-3:-adding-the-drill-jdbc-driver-to-squirrel)
  * [Step 4: Running a Drill Query from SQuirreL]({{ site.baseurl }}/docs/using-jdbc-with-squirrel-on-windows/#step-4:-running-a-drill-query-from-squirrel)

For information about how to use SQuirreL, refer to the [SQuirreL Quick
Start](http://squirrel-sql.sourceforge.net/user-manual/quick_start.html)
guide.
