---
title: "Using ODBC to Access Apache Drill from BI Tools"
parent: "Drill Interfaces"
---
MapR provides ODBC drivers for Windows, Mac OS X, and Linux. It is recommended
that you install the latest version of Apache Drill with the latest version of
the Drill ODBC driver.

For example, if you have Apache Drill 0.5 and a Drill ODBC driver installed on
your machine, and then you upgrade to Apache Drill 0.6, do not assume that the
Drill ODBC driver installed on your machine will work with the new version of
Apache Drill. Install the latest available Drill ODBC driver to ensure that
the two components work together.

You can access the latest Drill ODBC drivers in the following location:

`<http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/MapRDrill_odbc/>`

Refer to the following documents for driver installation and configuration
information, as well as examples for connecting to BI tools:

  * [Using the MapR ODBC Driver on Windows](/confluence/display/DRILL/Using+the+MapR+ODBC+Driver+on+Windows)
  * [Using the MapR Drill ODBC Driver on Linux and Mac OS X](/confluence/display/DRILL/Using+the+MapR+Drill+ODBC+Driver+on+Linux+and+Mac+OS+X)