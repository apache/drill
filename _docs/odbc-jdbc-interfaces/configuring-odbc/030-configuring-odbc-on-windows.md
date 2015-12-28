---
title: "Configuring ODBC on Windows"
date: 
parent: "Configuring ODBC"
---

Complete one of the following steps to create an ODBC connection on Windows to Drill data
sources:

  * Create a Data Source Name
  * Create an ODBC Connection String

## Sample ODBC Configuration (DSN)

You can see how to create a DSN to connect to Drill data sources by taking a look at the preconfigured sample that the installer sets up. If
you want to create a DSN for a 32-bit application, you must use the 32-bit
version of the ODBC Administrator to create the DSN.

1. Click **Start**, and locate the ODBC Administrator app that you installed, and click  ODBC Administrator to start the app.
   The ODBC Data Source Administrator dialog appears.
   ![]({{ site.baseurl }}/docs/img/odbc-user-dsn.png)

2. On the **System DSN** tab in System Data Sources, select the sample MapR ODBC Driver for Drill DSN.
   ![]({{ site.baseurl }}/docs/img/odbc-configure1.png)
   The system DSN is available for all users who log in to the machine. You can set up a user DSN is available only to the user who creates the DSN.  
3. Click **Configure**.  
   The MapR Drill ODBC Driver DSN Setup dialog appears with a preconfigured sample DSN. The following screenshot shows a possible DSN configuration for using Drill in embedded mode.
   ![]({{ site.baseurl }}/docs/img/odbc-configure2.png)
   
### Authentication Options
To password protect the DSN, uncomment the AuthenticationType, select Basic Authentication in the Authentication Type dropdown, and configure UID and PWD properties. To configure no password protection, select No Authentication.

### Direct to Drillbit and ZooKeeper Quorum Options
In the Connection Type section, Direct to Drillbit is selected for using Drill in embedded mode. To use Drill in embedded mode, set ConnectionType to Direct and define HOST and PORT properties. For example:

* `HOST=localhost`  
* `PORT=31010`

Check the `drill-override.conf` file for any port changes. 

To use Drill in distributed mode, select ZooKeeper Quorum. 

If you select ZooKeeper Quorum, provide values for the following properties:

* Quorum
  A comma separated list of ZooKeeper nodes in the following format:
  `<host name/ip address> : <port number>, <host name/ip address> : <port number>, . . .`
* Cluster ID
  Name of the drillbit cluster. Check the drill-override.conf file for ZooKeeper node information and for any cluster name changes.

 Check the `drill-override.conf` file for the cluster name.
![]({{ site.baseurl }}/docs/img/odbc-configure3.png)

The [Advanced Properties]({{site.baseurl}}/docs/odbc-configuration-reference/) section describes the advanced properties.

Select the Disable Async option to disable the asynchronous ODBC connection and enable a synchronous ODBC connection for performance reasons. By default the ODBC connection is asynchronous (Disable Asynch is not checked). A change in state occurs during driver initialization and is propagated to all driver DSNs.


[Logging Options]({{site.baseurl}}/docs/odbc-configuration-reference/#logging-options) and [Drill Explorer]({{site.baseurl}}/docs/drill-explorer-introduction/) sections describe the options at the bottom of this dialog.

### Next Step

[Testing the ODBC Connection]({{ site.baseurl }}/docs/testing-the-odbc-connection/).

