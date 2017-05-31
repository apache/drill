---
title: "Testing the ODBC Connection"
date: 2017-05-31 23:17:33 UTC
parent: "Configuring ODBC"
---
You can use DSN connection strings and DSN-less connection strings for your connections.

* [Example Connection String with DSN]({{site.baseurl}}/docs/testing-the-odbc-connection/#example-connection-string-with-DSN)
* [Example Connection Strings without DSN]({{site.baseurl}}/docs/testing-the-odbc-connection/#example-connection-strings-without-DSN)

## Example Connection String with DSN 
The following is an example of a connection string for a connection that uses a DSN:

    DSN=[DataSourceName]	

[*DataSourceName*] is the DSN that you are using for the connection.

You can set additional configuration options by appending key-value pairs to the connection string. Configuration options that are passed in using a connection string take precedence over configuration options that are set in the DSN.


## Example Connection Strings without DSN 

Some applications provide support for connecting to a data source using a driver without a DSN. To connect to a data source without using a DSN, use a connection string instead.
The placeholders in the examples are defined as follows, in alphabetical order:

* [*ClusterName*] is the name of the ZooKeeper cluster to which you are connecting.
* [*DomainName*] is the fully qualified domain name of the Drill server host.
* [*PortNumber*] is the number of the TCP port that the Drill server uses to listen for client connections.
* [*Server*] is the IP address or host name of the Drill server to which you are connecting.
* [*ServiceName*] is the Kerberos service principal name of the Drill server.
* [*YourPassword*] is the password corresponding to your user name.
* [*YourUserName*] is the user name that you use to access the Drill server.


###Drillbit Connections
The following is the format of a DSN-less connection string for a Drillbit that does not require authentication:

	Driver=MapR Drill ODBC Driver;ConnectionType=Direct; Host=[*Server*];Port=[*PortNumber*]

For example:

    Driver=MapR Drill ODBC Driver;ConnectionType=Direct; Host=192.168.222.160;Port=31010

###ZooKeeper Connections
The following is the format of a DSN-less connection string for a ZooKeeper cluster that does not require authentication:

Driver=MapR Drill ODBC Driver;
ConnectionType=ZooKeeper; 
ZKQuorum=[*Server1*]:[*PortNumber*1], [*Server2*]:[*PortNumber2*], [*Server3*]:[*PortNumber3*];
ZKClusterID=[*ClusterName*]

For example:

    Driver=MapR Drill ODBC Driver;
	ConnectionType=ZooKeeper; 
	ZKQuorum=192.168.222.160:31010, 192.168.222.165:31010, 192.168.222.231:31010;
	ZKClusterID=drill;


###Kerberos Authentication Connections
The following is the format of a DSN-less connection string for a Drillbit that requires Kerberos authentication:

Driver=MapR Drill ODBC Driver;
ConnectionType=Direct; 
Host=[*Server*];Port=[*PortNumber*];
AuthenticationType=Kerberos; 
KrbServiceHost=[*DomainName*];KrbServiceName=[*ServiceName*]

For example:

    Driver=MapR Drill ODBC Driver;
	ConnectionType=Direct; 
	Host=192.168.222.160;Port=31010;
	AuthenticationType=Kerberos; 
	KrbServiceHost=maprdriverdemo.example.com;	KrbServiceName=drill

###MapR-SASL Authentication Connections

The following is the format of a DSN-less connection string for a Drillbit that requires MapR-SASL authentication:

Driver=MapR Drill ODBC Driver;ConnectionType=Direct; Host=[*Server*];Port=[*PortNumber*];AuthenticationType=MapRSASL

For example:

    Driver=MapR Drill ODBC Driver;
	ConnectionType=Direct; 
	Host=192.168.227.169;
	Port=31010;
	AuthenticationType=MapRSASL

###Plain Authentication Connections

The following is the format of a DSN-less connection string for a Drillbit that requires Plain authentication:

	Driver=MapR Drill ODBC Driver;ConnectionType=Direct; 
	Host=[*Server*];Port=[*PortNumber*];	
	AuthenticationType=Plain;
	UID=[*YourUserName*];PWD=[*YourPassword*]

For example:

    Driver=MapR Drill ODBC Driver;ConnectionType=Direct; 
	Host=192.168.227.169;Port=31010;
    AuthenticationType=Plain;
	UID=username;PWD=mapr999


##Testing the ODBC Connection

The procedure for testing the ODBC connection differs depending on your platform, as described in the following sections:

* [Testing the ODBC Connection on Linux]({{site.baseurl}}/docs/testing-the-odbc-connection/#testing-the-odbc-connection-on-linux)
* [Testing the ODBC Connection on Mac OS X]({{site.baseurl}}/docs/testing-the-odbc-connection/#testing-the-odbc-connection-on-mac-os-x)
* [Testing the ODBC Connection on Windows]({{site.baseurl}}/docs/testing-the-odbc-connection/#testing-the-odbc-connection-on-windows)

## Testing the ODBC Connection on Linux

To test the ODBC connection on Linux use the test utilities in the `samples` directory of the driver manager installation: `iodbctest` and `iodbctestw`. Use `iodbctest` to test how your driver works with an ANSI application. Use `iodbctestw` to test how your driver works with a Unicode application.

There are 32-bit and 64-bit installations of the iODBC driver manager available. If you have only one or the other installed, then the appropriate version of iodbctest (or iodbctestw) is available. However, if you have both 32- and 64-bit versions installed, then you need to be careful that you are running the version from the correct installation directory.

See [http://www.iodbc.org](http://www.iodbc.org/) for further details on using the iODBC driver manager.

### Example of a Test on Linux

To test the ODBC connection on a Linux cluster, follow these steps:

1. [Start Drill]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/). For example, to start Drill in local mode on a Linux cluster:  

        [root@centos23 drill-1.10.0]# bin/drill-localhost
        apache drill 1.10.0 
        "the only truly happy people are children, the creative minority and drill users"

2. In the `samples` directory of the driver manager installation, run `iodbctest` or `iodbctestw`.  

        [root@centos23 libiodbc-3.52.7]# samples/iodbctest
          iODBC Demonstration program
        This program shows an interactive SQL processor
        Driver Manager: 03.52.0709.0909
   
   The prompt for an ODBC connection string appears.  

        Enter ODBC connect string (? shows list): ?

3. Type `?` to see the DSN name.  The output is:

        DSN                                          | Driver                                  
        ------------------------------------------------------------------------------
        Sample MapR Drill (64-bit)                   | MapR Drill ODBC Driver 64-bit           
        Enter ODBC connect string (? shows list):

4. To test the connection if the DSN was previously configured in the `.odbc.ini`, type the following connection string:  
 
        DSN=<DSN name>

      However, if you are connecting directly to a Drillbit and the DSN was not previously configured in the .odbc.ini, type an ODBC connection string using the following format:

        DSN=<DSN name>;
		ConnectionType=Direct;
		Host=<Host Name>;Port=<Port Number>
     
     OR
     
     If you are connecting to a ZooKeeper cluster, type an ODBC connection string using the following format:

        DSN=<DSN Name>;
		ConnectionType=ZooKeeper;
		ZKQuorum=<Server1:Port1>,<Server2:Port2>;
		ZKClusterID=<Cluster Name>

     The output of a successful test is:  

     `Driver: 1.3.8.<version> (MapR Drill ODBC Driver)  
     SQL> `  
     
     After the `SQL>` prompt appears, type `quit;`.
     Go to the Drill shell to run commands. Do not attempt to run SQL commands from this prompt.

This example shows a connection string for a direct connection:

        DSN=Sample MapR Drill 64-bit;
		ConnectionType=Direct;
		Host=localhost;Port=31010

This example shows a connection string for a ZooKeeper cluster connection:

        DSN=Sample MapR Drill 64-bit 64;
		ConnectionType=ZooKeeper;
		ZKQuorum=centos23.lab:5181;
		ZKClusterID=docs60cluster-drillbits

## Testing the ODBC Connection on Mac OS X

To test the ODBC connection on Mac OS X, follow these steps.

1. [Start Drill]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/). 
 
2. Start the iODBC Administrator.app in `/Applications`.  
   The iODBC Data Source Administrator dialog appears. 

3. On the User DSN tab, select **MapR Drill**.  
   ![]({{ site.baseurl }}/docs/img/odbc_data_source_names.png) 

4. Click **Test**.  
   The Login for the MapR Drill dialog appears.  
   ![]({{ site.baseurl }}/docs/img/odbc_login.png)  

5. If you configured Plain (or Basic) authentication in the `.odbc.ini`, enter the user name and password you also configured; otherwise, click **OK**.  The success message displays.  
   ![]({{ site.baseurl }}/docs/img/success.png)
   
## Testing the ODBC Connection on Windows

To test the ODBC connection on Windows, follow these steps:

1. Follow instructions to configure the ODBC connection on Windows. 
   The **MapR Drill ODBC Driver DSN Setup** dialog is displayed.

2. Click **Test**. A Test Results popup will display that states you have successfully connected to the data source. 

	![](http://i.imgur.com/Hxuutmq.png)

