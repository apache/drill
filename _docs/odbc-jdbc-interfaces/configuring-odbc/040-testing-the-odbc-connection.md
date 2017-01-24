---
title: "Testing the ODBC Connection"
date: 2017-01-24 22:24:25 UTC
parent: "Configuring ODBC"
---
The procedure for testing the ODBC connection differs depending on your platform, as described in the following sections:

* [Testing the ODBC Connection on Linux]({{site.baseurl}}/docs/testing-the-odbc-connection/#testing-the-odbc-connection-on-linux)
* [Testing the ODBC Connection on Mac OS X]({{site.baseurl}}/docs/testing-the-odbc-connection/#testing-the-odbc-connection-on-mac-os-x)
* [Testing the ODBC Connection on Windows]({{site.baseurl}}/docs/testing-the-odbc-connection/#testing-the-odbc-connection-on-windows)

## Testing the ODBC Connection on Linux

To test the ODBC connection on Linux use the test utilities in the `samples` directory of the driver manager installation: `iodbctest` and `iodbctestw`. Use `iodbctest` to
test how your driver works with an ANSI application. Use `iodbctestw` to test
how your driver works with a Unicode application.

There are 32-bit and 64-bit installations of the iODBC driver manager available. If you have only one or the other installed, then the appropriate version of iodbctest (or iodbctestw) is available. However, if you have both 32- and 64-bit versions installed, then you need to be careful that you are running the version from the correct installation directory.

Visit [http://www.iodbc.org](http://www.iodbc.org/) for further details on
using the iODBC driver manager.

### Example of a Test on Linux

To test the ODBC connection on a Linux cluster, follow these steps:

1. [Start Drill]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/). For example, to start Drill in local mode on a linux cluster:  

        [root@centos23 drill-1.2.0]# bin/drill-localhost
        apache drill 1.2.0 
        "the only truly happy people are children, the creative minority and drill users"

2. In the `samples` directory of the driver manager installation, run `iodbctest` or `iodbctestw`.  

        [root@centos23 libiodbc-3.52.7]# samples/iodbctest
          iODBC Demonstration program
        This program shows an interactive SQL processor
        Driver Manager: 03.52.0709.0909
   The prompt for an ODBC connection string appears.  

        Enter ODBC connect string (? shows list): ?

3. Type ? to see the DSN name.  
   Output is:

        DSN                              | Driver                                  
        ------------------------------------------------------------------------------
        Sample MapR Drill DSN 64         | MapR Drill ODBC Driver 64-bit           
        Enter ODBC connect string (? shows list):

4. If you are connecting directly to a Drillbit, type an ODBC connection string using the following format:

        DSN=<DSN name>;ConnectionType=Direct;Host=<Host Name>;Port=<Port Number>
     
     OR
     
     If you are connecting to a ZooKeeper cluster, type an ODBC connection string using the following format:

        DSN=<DSN Name>;ConnectionType=ZooKeeper;ZKQuorum=<Server1:Port1>,<Server2:Port2>;ZKClusterID=<Cluster Name>`

     The output of a successful test is:  
     `Driver: 1.2.0.1001 (MapR Drill ODBC Driver)  
     SQL> `  
     After the `SQL>` prompt appears, type `quit;`, and go to the Drill shell to run commands. Do not attempt to run SQL commands from this prompt.

### Example Connection Strings

The following example shows a connection string for a direct connection:

        DSN=Sample MapR Drill DSN 64;ConnectionType=Direct;Host=localhost;Port=31010

The following example shows a connection string for a ZooKeeper cluster connection:

        DSN=Sample MapR Drill DSN 64;ConnectionType=ZooKeeper;ZKQuorum=centos23.lab:5181;ZKClusterID=docs41cluster-drillbits

## Testing the ODBC Connection on Mac OS X

To test the ODBC connection on Mac OS X, follow these steps.

1. [Start Drill]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/).  
2. Start the iODBC Administrator.app in `/Applications`.  
   The iODBC Data Source Administrator dialog appears. 
3. On the User DSN tab, select the MapR Drill.  
   ![]({{ site.baseurl }}/docs/img/odbc_data_source_names.png)  
4. Click **Test**.  
   The Login for Sample MapR Drill dialog appears.  
   ![]({{ site.baseurl }}/docs/img/odbc_login.png)  
5. If you configured Basic Authentication in the `.odbc.ini`, enter the user name and password you also configured; otherwise, click **OK**.  
   The success message appears.  
   ![]({{ site.baseurl }}/docs/img/success.png)  
   
## Testing the ODBC Connection on Windows

To test the ODBC connection on Windows, follow these steps:

1. Follow instructions to configure the ODBC connection on Windows.  
   The MapR Drill ODBC Driver DSN Setup dialog is displayed.
   ![]({{ site.baseurl }}/docs/img/odbc-configure2.png)

2. Click the Test Button.
   ![]({{ site.baseurl }}/docs/img/odbc-test.png)
