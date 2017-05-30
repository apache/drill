---
title: "Configuring ODBC on Windows"
date: 2017-05-30 23:11:53 UTC
parent: "Configuring ODBC"
---
Complete one of the following steps to create an ODBC connection on Windows to Drill data
sources:

  * Create a Data Source Name
  * Create an ODBC Connection String

## Sample ODBC Configuration

You can see how to create a DSN to connect to Drill data sources by taking a look at the preconfigured sample that the installer sets up. If you want to create a DSN for a 32-bit application, you must use the 32-bit
version of the ODBC Administrator to create the DSN.

1. Click **Start** and locate the ODBC Administrator app that you installed. Then click ODBC Administrator to start the app.
   The ODBC Data Source Administrator dialog appears.
   ![]({{ site.baseurl }}/docs/img/odbc-user-dsn.png)

2. On the **System DSN** tab in System Data Sources, select the sample MapR Drill.
   ![]({{ site.baseurl }}/docs/img/odbc-configure1.png)
   The system DSN is available for all users who log in to the machine. You can set up a user DSN is available only to the user who creates the DSN on the **User DSN** tab.  
3. Click **Configure**.  
   The MapR Drill setup dialog appears with a preconfigured sample DSN. The following screenshot shows a possible DSN configuration for using Drill in embedded mode.
   ![]({{ site.baseurl }}/docs/img/odbc-configure2.png)
   
### Authentication Options
To password protect the DSN, select the appropriate AuthenticationType in the dropdown.  If the Drillbit does not require authentication (or to configure no password protection), you can use the No Authentication option; you do not need to configure additional settings.

* **MapR-SASL**
	* The maprlogin utility must be used to obtain a MapR ticket. To install and use the MapR login utility, see <a href="http://maprdocs.mapr.com/home/SecurityGuide/SecurityArchitecture-AuthenticationArchitecture.html" title="MapR Login Utilty">Authentication Architecture: The maprlogin Utility</a> and <a href="http://maprdocs.mapr.com/home/SecurityGuide/Tickets.html/">Tickets</a>.
* **Kerberos** - configure Host FQDN and Service Name properties.
	* To specify the default Kerberos mechanism, select the Use Only SSPI checkbox.
	* To use MIT Kerberos by default and only use the SSPI plugin if the GSSAPI library is not available, clear the Use Only SSPI checkbox.
* **Plain Authentication** - configure UID and PWD properties. 


### Direct to Drillbit and ZooKeeper Quorum Options
In the Connection Type section, Direct to Drillbit is selected for using Drill in embedded mode. To use Drill in embedded mode, set ConnectionType to Direct and define HOST and PORT properties. For example:

* `HOST=localhost`  
* `PORT=31010`

Check the `drill-override.conf` file for any port changes. 

To use Drill in distributed mode, select ZooKeeper Quorum. 

If you select ZooKeeper Quorum, provide values for the following properties:

* Quorum - 
  A comma separated list of ZooKeeper nodes in the following format:
  `<host name/ip address> : <port number>, <host name/ip address> : <port number>, . . .`
* Cluster ID - 
  Name of the drillbit cluster. Check the drill-override.conf file for ZooKeeper node information and for any cluster name changes.

 Check the `drill-override.conf` file for the cluster name.
![]({{ site.baseurl }}/docs/img/odbc-configure3.png)

The [Advanced Properties]({{site.baseurl}}/docs/odbc-configuration-reference/) section describes the advanced properties.

Select the Disable Async option to disable the asynchronous ODBC connection and enable a synchronous ODBC connection for performance reasons. By default the ODBC connection is asynchronous (Disable Asynch is not checked). A change in state occurs during driver initialization and is propagated to all driver DSNs.


[Logging Options]({{site.baseurl}}/docs/odbc-configuration-reference/#logging-options) and [Drill Explorer]({{site.baseurl}}/docs/drill-explorer-introduction/) sections describe the options at the bottom of this dialog.

### Next Step

[Testing the ODBC Connection]({{ site.baseurl }}/docs/testing-the-odbc-connection/).
