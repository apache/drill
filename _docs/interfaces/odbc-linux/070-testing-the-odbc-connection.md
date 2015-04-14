---
title: "Testing the ODBC Connection"
parent: "Using ODBC on Linux and Mac OS X"
---
To test the ODBC connection on Linux and Mac OS X, you can use an ODBC-enabled client application. For a
basic connection test, you can also use the test utilities that are packaged
with your driver manager installation.

For example, the iODBC driver manager includes simple utilities called
`iodbctest` and `iodbctestw`. You can use either one of these utilities to
establish a test connection with your driver and your DSN. Use `iodbctest` to
test how your driver works with an ANSI application. Use `iodbctestw` to test
how your driver works with a Unicode application.

**Note:** There are 32-bit and 64-bit installations of the iODBC driver manager available. If you have only one or the other installed, then the appropriate version of iodbctest (or iodbctestw) is available. However, if you have both 32- and 64-bit versions installed, then you need to be careful that you are running the version from the correct installation directory.

Visit [http://www.iodbc.org](http://www.iodbc.org/) for further details on
using the iODBC driver manager.

## Testing the ODBC Connection

Complete the following steps to test your connection using the iODBC driver
manager:

  1. Run `iodbctest` or `iodbctestw`. The program prompts you for an ODBC connection string.
  2. If you do not remember the DSN name, type a question mark (?) to see a list of DSNs.
  3. If you are connecting directly to a Drillbit, type an ODBC connection string using the following format:

     `DRIVER=MapR Drill ODBC Driver;ConnectionType=Direct;Host=HostName;Port=PortNumber`
     
     OR
     
     If you are connecting to a ZooKeeper cluster, type an ODBC connection string
using the following format:

     `DRIVER=MapR Drill ODBC Driver;ConnectionType=ZooKeeper;ZKQuorum=Server1:Port1
,Server2:Port2;ZKClusterID=DrillbitName`

     If the connection is successful, the `SQL>` prompt appears.

