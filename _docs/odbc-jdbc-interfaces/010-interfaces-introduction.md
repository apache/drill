---
title: "Interfaces Introduction"
parent: "ODBC/JDBC Interfaces"
---
You can connect to Apache Drill through the following interfaces:

  * Drill shell
  * Drill Web UI
  * [ODBC]({{ site.baseurl }}/docs/installing-the-odbc-driver/)*
  * [JDBC]({{ site.baseurl }}/docs/using-jdbc/)
  * C++ API

*Apache Drill does not have an open source ODBC driver. However, MapR provides an ODBC 3.8 driver developed specifically for connecting Apache Drill to BI tools. 

## Using ODBC to Access Apache Drill from BI Tools

MapR provides an ODBC 3.8 driver that connects Windows, Mac OS X, and Linux to Apache Drill and BI tools. Install the latest version of Apache Drill with the latest version of
the MapR Drill ODBC driver. For example, if you have Apache Drill 0.8 and a MapR Drill ODBC driver installed on your machine, and you upgrade to Apache Drill 1.0, do not assume that the
MapR Drill ODBC driver installed on your machine will work with the new version of
Apache Drill. Use latest MapR Drill ODBC driver and Apache Drill versions.

Access the latest MapR Drill ODBC drivers in the following location:

<http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc>

## Using JDBC to Access Apache Drill from SQuirrel

You can connect to Drill through a JDBC client tool, such as SQuirreL, on
Windows, Linux, and Mac OS X systems, to access all of your data sources
registered with Drill. An embedded JDBC driver is included with Drill.
Configure the JDBC driver in the SQuirreL client to connect to Drill from
SQuirreL. This document provides instruction for connecting to Drill from
SQuirreL on Windows.

To use the Drill JDBC driver with SQuirreL on Windows, complete the following
steps:

  * [Step 1: Getting the Drill JDBC Driver]({{ site.baseurl }}/docs/using-jdbc#step-1:-getting-the-drill-jdbc-driver) 
  * [Step 2: Installing and Starting SQuirreL]({{ site.baseurl }}/docs/using-jdbc#step-2:-installing-and-starting-squirrel)
  * [Step 3: Adding the Drill JDBC Driver to SQuirreL]({{ site.baseurl }}/docs/using-jdbc#step-3:-adding-the-drill-jdbc-driver-to-squirrel)
  * [Step 4: Running a Drill Query from SQuirreL]({{ site.baseurl }}/docs/using-jdbc#step-4:-running-a-drill-query-from-squirrel)

For information about how to use SQuirreL, refer to the [SQuirreL Quick
Start](http://squirrel-sql.sourceforge.net/user-manual/quick_start.html)
guide.
