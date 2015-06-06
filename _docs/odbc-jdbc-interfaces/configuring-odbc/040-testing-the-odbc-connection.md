---
title: "Testing the ODBC Connection"
parent: "Configuring ODBC"
---
The procedure for testing the ODBC connection differs depending on your platform, as described in the following sections:

* [Testing the ODBC Connection on Mac OS X](/docs/testing-the-odbc-connection/#testing-the-odbc-connection-on-mac-os-x)
* [Testing the ODBC Connection on Linux](/docs/testing-the-odbc-connection/#testing-the-odbc-connection-on-linux)
* [Testing the ODBC Connection on Windows](/docs/testing-the-odbc-connection/#testing-the-odbc-connection-on-windows)

## Testing the ODBC Connection on Mac OS X

To test the ODBC connection Mac OS X, follow these steps.

1. [Start Drill]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/).  
2. Start the iODBC Administrator.app in `/Applications`.  
   The iODBC Data Source Administrator dialog appears.  
2. On the User DSN tab, select the MapR Drill DSN.  
   ![]({{ site.baseurl }}/docs/img/odbc-mac1.png)  
3. Click **Test**.  
   The Login for Sample MapR Drill DSN dialog appears.  
   ![]({{ site.baseurl }}/docs/img/odbc-mac2.png)  
4. If you configured Basic Authentication in the .odbc.ini, enter the user name and password you also configured; otherwise, click OK.  
   The success message appears.  
   ![]({{ site.baseurl }}/docs/img/odbc-mac3.png)  

## Testing the ODBC Connection on Linux

To test the ODBC connection on Linux use the test utilities that are packaged
with your driver manager installation: `iodbctest` and `iodbctestw`. Use `iodbctest` to
test how your driver works with an ANSI application. Use `iodbctestw` to test
how your driver works with a Unicode application.

There are 32-bit and 64-bit installations of the iODBC driver manager available. If you have only one or the other installed, then the appropriate version of iodbctest (or iodbctestw) is available. However, if you have both 32- and 64-bit versions installed, then you need to be careful that you are running the version from the correct installation directory.

Visit [http://www.iodbc.org](http://www.iodbc.org/) for further details on
using the iODBC driver manager.

To test the ODBC connection on Linux, follow these steps:

1. [Start Drill]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/).
2. Run `iodbctest` or `iodbctestw`. The program prompts you for an ODBC connection string.
2. If you do not remember the DSN name, type a question mark (?) to see a list of DSNs.
3. If you are connecting directly to a Drillbit, type an ODBC connection string using the following format:

     `DRIVER=MapR Drill ODBC Driver;ConnectionType=Direct;Host=HostName;Port=PortNumber`
     
     OR
     
     If you are connecting to a ZooKeeper cluster, type an ODBC connection string
using the following format:

     `DRIVER=MapR Drill ODBC Driver;ConnectionType=ZooKeeper;ZKQuorum=Server1:Port1
,Server2:Port2;ZKClusterID=DrillbitName`

     If the connection is successful, the `SQL>` prompt appears.

## Testing the ODBC Connection on Windows

To test the ODBC connection on Windows, follow these steps:

1. [Start Drill]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/).