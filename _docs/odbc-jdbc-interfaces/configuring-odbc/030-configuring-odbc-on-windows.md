---
title: "Configuring ODBC on Windows"
date: 2017-08-17 18:55:12 UTC
parent: "Configuring ODBC"
---
To create an ODBC connection to Drill data sources on Windows, complete the following steps:

  * [Step 1: Create a Data Source Name (DSN)]({{site.baseurl}}/docs/configuring-odbc-on-windows/#step-1:-create-a-data-source-name-(dsn))
  * [Step 2: Select an Authentication Option]({{site.baseurl}}/docs/configuring-odbc-on-windows/#step-2:-select-an-authentication-option)  
  * [Step 3: Configure the Connection Type]({{site.baseurl}}/docs/configuring-odbc-on-windows/#step-3:-configure-the-connection-type)  
  * [Step 4: Configure Advanced Properties (optional)]({{site.baseurl}}/docs/configuring-odbc-on-windows/#step-4:-configure-advanced-properties-(optional))  
  * [Additional Configuration Options]({{site.baseurl}}/docs/configuring-odbc-on-windows/#additional-configuration-options)

## Step 1: Create a Data Source Name (DSN) 

You can see how to create a DSN to connect to Drill data sources by taking a look at the preconfigured sample that the installer sets up. If you want to create a DSN for a 32- or 64-bit application, you must use the 32- or 64-bit
version of the ODBC Data Source Administrator to create the DSN.

1\. From the Settings dialog in Windows 10, enter **odbc** and select **Setup ODBC data sources `<version>`**. The ODBC Data Source Administrator `<version>` dialog appears.   

![](http://i.imgur.com/uK42pUe.png)



2\. On the **System DSN** tab in System Data Sources, select **MapR Drill**.  

   The system DSN is available for all users who log in to the machine. A user DSN is available only to the user who creates the DSN on the **User DSN** tab. 
 
3\. Click **Configure**.  
  
   The **MapR Drill ODBC Driver DSN Setup** dialog appears with a preconfigured sample DSN. The following image shows a possible DSN configuration for using Drill in embedded mode.  


![](http://i.imgur.com/f9Avhcz.png) 

   To access Drill Explorer, click **Drill Explorer...**. See [Drill Explorer]({{site.baseurl}}/docs/drill-explorer-introduction/) for more information.
   
### Step 2: Select an Authentication Option
To password protect the DSN, select the appropriate authentication type in the **Authentication Type** dropdown.  If the Drillbit does not require authentication (or to configure no password protection), you can use the No Authentication option. You do not need to configure additional settings.

* **Kerberos** - configure Host FQDN and Service Name properties.
	* To specify the default Kerberos mechanism, select the Use Only SSPI checkbox.
	* To use MIT Kerberos by default and only use the SSPI plugin if the GSSAPI library is not available, clear the Use Only SSPI checkbox.
* **Plain Authentication** - configure UID and PWD properties. 

### Step 3: Configure the Connection Type
In the **Connection Type** section, **Direct to Drillbit** is selected for using Drill in embedded mode. To use Drill in embedded mode, set the connection type to **Direct** and define HOST and PORT properties. For example:

* `HOST=localhost`  
* `PORT=31010`

Check the `drill-override.conf` file for any port changes. 

To use Drill in distributed mode, select **ZooKeeper Quorum**. If you select ZooKeeper Quorum, provide values for the following properties:

* **Quorum** - 
  A comma separated list of ZooKeeper nodes in the following format:
  `<host name/ip address> : <port number>, <host name/ip address> : <port number>, . . .`

* **Cluster ID** - 
  Name of the drillbit cluster. Check the `drill-override.conf` file for ZooKeeper node information and for any cluster name changes.

 Check the `drill-override.conf` file for the cluster name.  

## Step 4: Configure Advanced Properties (optional)


The [Advanced Properties]({{site.baseurl}}/docs/odbc-configuration-reference/) section describes the advanced configuration properties in detail.  

## Additional Configuration Options

Select the **Disable Async** option to disable the asynchronous ODBC connection and enable a synchronous ODBC connection for performance reasons. By default the ODBC connection is asynchronous (Disable Asynch is not checked). A change in state occurs during driver initialization and is propagated to all driver DSNs.

For information about logging options, see [Logging Options]({{site.baseurl}}/docs/odbc-configuration-reference/#logging-options).

### Next Step

[Testing the ODBC Connection]({{ site.baseurl }}/docs/testing-the-odbc-connection/).
