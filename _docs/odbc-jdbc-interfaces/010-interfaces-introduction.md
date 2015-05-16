---
title: "Interfaces Introduction"
parent: "ODBC/JDBC Interfaces"
---
You can connect to Apache Drill through the following interfaces:

  * Drill shell (SQLLine)
  * Drill Web UI
  * [ODBC]({{ site.baseurl }}/docs/odbc-jdbc-interfaces#using-odbc-to-access-apache-drill-from-bi-tools)*
  * [JDBC]({{ site.baseurl }}/docs/using-jdbc/)
  * C++ API

*Apache Drill does not have an open source ODBC driver. However, MapR provides an ODBC driver that you can use to connect to Apache Drill from BI tools. 

## Using ODBC to Access Apache Drill from BI Tools

MapR provides ODBC drivers for Windows, Mac OS X, and Linux. It is recommended
that you install the latest version of Apache Drill with the latest version of
the Drill ODBC driver.

For example, if you have Apache Drill 0.8 and a Drill ODBC driver installed on
your machine, and then you upgrade to Apache Drill 1.0, do not assume that the
Drill ODBC driver installed on your machine will work with the new version of
Apache Drill. Install the latest available Drill ODBC driver to ensure that
the two components work together.

You can access the latest Drill ODBC drivers in the following location:

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
