---
title: "Interfaces Introduction"
date: 2018-10-30 21:47:12 UTC
parent: "ODBC/JDBC Interfaces"
---
You can connect to Apache Drill through the following interfaces:

  * Drill shell
  * Drill Web Console
  * [ODBC]({{ site.baseurl }}/docs/installing-the-odbc-driver/)*
  * [JDBC]({{ site.baseurl }}/docs/using-jdbc-with-squirrel-on-windows/)
  * C++ API

*Apache Drill does not have an open source ODBC driver. However, MapR provides an [ODBC driver](https://package.mapr.com/tools/MapR-ODBC/MapR_Drill/) developed specifically for connecting Apache Drill to BI tools. MapR also provides a [JDBC driver](https://package.mapr.com/tools/MapR-JDBC/MapR_Drill/).  

## Using ODBC to Access Apache Drill from BI Tools

MapR provides an ODBC driver that connects Windows, Mac OS X, and Linux to Apache Drill and BI tools. Install the latest version of Apache Drill with the latest version of the MapR Drill ODBC driver. 

Access the latest MapR Drill ODBC drivers at [ODBC driver](https://package.mapr.com/tools/MapR-ODBC/MapR_Drill/).

## Using JDBC to Access Apache Drill from SQuirreL

You can connect to Drill through a JDBC client tool, such as SQuirreL, on
Windows, Linux, and Mac OS X systems, to access all of your data sources
registered with Drill. You can use the [Drill JDBC driver provided by MapR](https://package.mapr.com/tools/MapR-JDBC/MapR_Drill/), or the embedded JDBC driver included with Drill.

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
