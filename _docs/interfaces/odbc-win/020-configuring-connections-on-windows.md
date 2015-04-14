---
title: "Configuring Connections on Windows"
parent: "Using ODBC on Windows"
---
Complete one of the following steps to create an ODBC connection on Windows to Drill data
sources:

  * Create a Data Source Name
  * Create an ODBC Connection String

**Prerequisite:** An Apache Drill installation must be available that is configured to access the data sources that you want to connect to.  For information about how to install Apache Drill, see [Install Drill](/docs/install-drill). For information about configuring data sources, see the [Apache Drill documentation](/docs).

## Create a Data Source Name (DSN)

Create a DSN that an application can use to connect to Drill data sources. If
you want to create a DSN for a 32-bit application, you must use the 32-bit
version of the ODBC Administrator to create the DSN.

  1. To launch the ODBC Administrator, click **Start > All Programs > MapR Drill ODBC Driver 1.0 (32|64-bit) > (32|64-bit) ODBC Administrator**.  
The ODBC Data Source Administrator window appears.

     To launch the 32-bit version of the ODBC driver on a 64-bit machine, run:
`C:\WINDOWS\SysWOW64\odbcad32.exe`.
  2. Click the **System DSN** tab to create a system DSN or click the **User DSN** tab to create a user DSN. A system DSN is available for all users who log in to the machine. A user DSN is available to the user who creates the DSN.
  3. Click **Add**.
  4. Select **MapR Drill ODBC Driver** and click **Finish**.  
     The _MapR Drill ODBC Driver DSN Setup_ window appears.
  5. In the **Data Source Name** field, enter a name for the DSN,
  6. Optionally, enter a description of the DSN in the Description field.
  7. In the Connection Type section, select a connection type and enter the associated connection details:

     <table style='table-layout:fixed;width:100%'><tbody><tr><th>Connection Type</th><th >Properties</th><th >Descriptions</th></tr><tr><td rowspan="2" valign="top" width="10%">Zookeeper Quorum</td><td valign="top" style='width: 100px;'>Quorum</td><td valign="top" style='width: 400px;'>A comma-separated list of servers in a Zookeeper cluster.For example, &lt;ip_zookeepernode1&gt;:5181,&lt;ip_zookeepernode21&gt;:5181,…</td></tr><tr><td valign="top">ClusterID</td><td valign="top">Name of the drillbit cluster. The default is drillbits1. You may need to specify a different value if the cluster ID was changed in the drill-override.conf file.</td></tr><tr><td colspan="1" valign="top">Direct to Drillbit</td><td colspan="1" valign="top"> </td><td colspan="1" valign="top">Provide the IP address or host name of the Drill server and the port number that that the Drill server is listening on.  The port number defaults to 31010. You may need to specify a different value if the port number was changed in the drill-override.conf file.</td></tr></tbody></table>
     For information on selecting the appropriate connection type, see [Connection
Types](/docs/step-2-configure-odbc-connections-to-drill-data-sources#connection-type).
  8. In the **Default Schema** field, select the default schema that you want to connect to.
     For more information about the schemas that appear in this list, see Schemas.
  9. Optionally, perform one of the following operations:

     <table ><tbody><tr><th >Option</th><th >Action</th></tr><tr><td valign="top">Update the configuration of the advanced properties.</td><td valign="top">Edit the default values in the <strong>Advanced Properties</strong> section. <br />For more information, see <a href="/docs/advanced-properties/">Advanced Properties</a>.</td></tr><tr><td valign="top">Configure the types of events that you want the driver to log.</td><td valign="top">Click <strong>Logging Options</strong>. <br />For more information, see <a href="/docs/step-2-configure-odbc-connections-to-drill-data-sources#logging-options">Logging Options</a>.</td></tr><tr><td valign="top">Create views or explore Drill sources.</td><td valign="top">Click <strong>Drill Explorer</strong>. <br />For more information, see <a href="/docs/using-drill-explorer-to-browse-data-and-create-views">Using Drill Explorer to Browse Data and Create Views</a>.</td></tr></tbody></table>
  10. Click **OK** to save the DSN.

## Configuration Options

### Connection Type

ODBC can connect directly to a Drillbit or to a ZooKeeper Quorum. Select your
connection type based on your environment and Drillbit configuration.

The following table lists the appropriate connection type for each scenario:

<table ><tbody><tr><th >Scenario</th><th >Connection Type</th></tr><tr><td valign="top">Drillbit is running in embedded mode.</td><td valign="top">Direct to Drillbit</td></tr><tr><td valign="top">Drillbit is registered with the ZooKeeper in a testing environment.</td><td valign="top">ZooKeeper Quorum or Direct to Drillbit</td></tr><tr><td valign="top">Drillbit is registered with the ZooKeeper in a production environment.</td><td valign="top">ZooKeeper Quorum</td></tr></tbody></table> 

#### Connection to Zookeeper Quorum

When you choose to connect to a ZooKeeper Quorum, the ODBC driver connects to
the ZooKeeper Quorum to get a list of available Drillbits in the specified
cluster. Then, the ODBC driver submits a query after selecting a Drillbit. All
Drillbits in the cluster process the query and the Drillbit that received the
query returns the query results.

![ODBC to Quorum]({{ site.baseurl }}/docs/img/ODBC_to_Quorum.png)

In a production environment, you should connect to a ZooKeeper Quorum for a
more reliable connection. If one Drillbit is not available, another Drillbit
that is registered with the ZooKeeper quorum can accept the query.

#### Direct Connection to Drillbit

When you choose to connect directly to a Drillbit, the ODBC driver connects to
the Drillbit and submits a query. If you connect directly to Drillbit that is
not part of a cluster, the Drillbit that you connect to processes the query.
If you connect directly to a Drillbit that is part of a cluster, all Drillbits
in the cluster process the query. In either case, the Drillbit that the ODBC
driver connected to returns the query results.

![]({{ site.baseurl }}/docs/img/ODBC_to_Drillbit.png)

### Catalog

This value defaults to DRILL and cannot be changed.

### Schema

The Default Schema list contains the data sources that you have configured to
use with Drill via the Drill Storage Plugin.

Views that you create using the Drill Explorer do not appear under the schema
associated with the data source type. Instead, the views can be accessed from
the file-based schema that you selected when saving the view.

### Advanced Properties

The Advanced Properties field allows you to customize the DSN.  
You can configure the values of the following advanced properties:

<table ><tbody><tr><th >Property Name</th><th >Default Value</th><th >Description</th></tr><tr><td valign="top">HandshakeTimeout</td><td valign="top">5</td><td valign="top">An integer value representing the number of seconds that the driver waits to establish a connection before aborting. When set to 0, the driver does not abort connection attempts.</td></tr><tr><td valign="top">QueryTimeout</td><td valign="top">180</td><td valign="top">An integer value representing the number of seconds for the driver to wait before automatically stopping a query. When set to 0, the driver does not stop queries automatically.</td></tr><tr><td valign="top">TimestampTZDisplayTimezone</td><td valign="top">local</td><td valign="top">A string value that defines how the timestamp with timezone is displayed:<ul><li class="Body"><strong>local</strong>—Timestamps appear in the time zone of the user.</li><li class="Body"><strong>utc</strong>—Timestamps appear in Coordinated Universal Time (UTC).</li></ul></td></tr><tr><td valign="top">ExcludedSchemas</td><td valign="top">sys, INFORMATION_SCHEMA</td><td valign="top">A list of schemas that should not appear in client applications such as Drill Explorer, Tableau, and Excel. Separate schemas in the list using a comma (,).</td></tr></tbody></table> 
Separate each advanced property using a semicolon.

For example, the following Advanced Properties string excludes the schemas
named test and abc; sets the timeout to 30 seconds; and, sets the time zone to
Coordinated Universal Time:  
`HandshakeTimeout=30;QueryTimeout=30;TimestampTZDisplayTimezone=utc;ExcludedSchemas=test,abc`

### Logging Options

Configure logging to troubleshoot issues. To configure logging, click the
Logging Options button on the ODBC DSN Setup dialog and then set a log level
and a log path.

If logging is enabled, the MapR Drill ODBC driver logs events in following log
files in the log path that you configure:

<table ><tbody><tr><th >Log File</th><th >Description</th></tr><tr><td valign="top">driver.log</td><td valign="top">A log of driver events.</td></tr><tr><td valign="top">drillclient.log</td><td valign="top">A log of the Drill client events.</td></tr></tbody></table> 

#### Logging Levels

Each logging level provides a different level of detail in the log files. The
following log levels are available:

<table ><tbody><tr><th >Logging Level</th><th >Description</th></tr><tr><td valign="top">OFF</td><td valign="top">Disables all logging.</td></tr><tr><td valign="top">FATAL</td><td valign="top">Logs severe error events that may cause the driver to stop running.</td></tr><tr><td valign="top">ERROR</td><td valign="top">Logs error events that may allow the driver to continue running.</td></tr><tr><td valign="top">WARNING</td><td valign="top">Logs events about potentially harmful situations.</td></tr><tr><td valign="top">INFO</td><td valign="top">Logs high-level events about driver processes.</td></tr><tr><td valign="top">DEBUG</td><td valign="top">Logs detailed events that may help to debug issues.</td></tr><tr><td colspan="1" valign="top">TRACE</td><td colspan="1" valign="top">Logs finer-grained events than the DEBUG level.</td></tr></tbody></table>

## Create an ODBC Connection String

If you want to connect to a Drill data source from an application that does
not require a DSN, you can use an ODBC connection string.  
The following table describes the properties that you can use in the
connection string:

<table ><tbody><tr><th >Property</th><th >Description</th></tr><tr><td valign="top">AdvancedProperties</td><td valign="top">Separate advanced properties using a semicolon (;), and then surround all advanced properties in a connection string using braces { and }.For more information, see <a href="#Step2.ConfigureODBCConnectionstoDrillDataSources-AdvancedProperties">Advanced Properties</a>.</td></tr><tr><td valign="top">Catalog</td><td valign="top">The name of the catalog, under which all of the schemas are organized. The catalog name is DRILL.</td></tr><tr><td valign="top">ConnectionType</td><td valign="top">One of the following values:<br />• Direct—Connect to a Drill server using Host and Port properties in the connection string.<br />• ZooKeeper—Connect to a ZooKeeper cluster using ZKQuorum and ZKClusterID properties in the connection string.For more information, see<a href="#Step2.ConfigureODBCConnectionstoDrillDataSources-ConnectionType"> Connection Type</a>.</td></tr><tr><td valign="top">DRIVER</td><td valign="top">The name of the installed driver: MapR Drill ODBC Driver<br />(Required)</td></tr><tr><td valign="top">Host</td><td valign="top">If the ConnectionType property is set to Direct, indicate the IP address or hostname of the Drillbit server.</td></tr><tr><td valign="top">Port</td><td valign="top">If the ConnectionType property is set to Direct, indicate the port on which the Drillbit server is listening.</td></tr><tr><td valign="top">Schema</td><td valign="top">The name of the database schema to use when a schema is not explicitly specified in a query.<br />Note: Queries on other schemas can still be issued by explicitly specifying the schema in the query.</td></tr><tr><td valign="top">ZKClusterID</td><td valign="top">If the ConnectionType property is set to ZooKeeper, then use ZKClusterID to indicate the name of the Drillbit cluster to use.</td></tr><tr><td valign="top">ZKQuorum</td><td valign="top">If the ConnectionType property is set to ZooKeeper, then use ZKQuorum to indicate the server(s) in your ZooKeeper cluster. Separate multiple servers using a comma (,).</td></tr></tbody></table>

#### Connection String Examples

The following is an example connection string for the Direct connection type:  

        DRIVER=MapR Drill ODBC Driver;AdvancedProperties={HandshakeTimeout=0;QueryTimeout=0;TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys,INFORMATION_SCHEMA;};Catalog=DRILL;Schema=hivestg;ConnectionType=Direct;Host=192.168.202.147;Port=31010

The following is an example connection string for the Zookeeper connection
type:  

        DRIVER=MapR Drill ODBC Driver;AdvancedProperties={HandshakeTimeout=0;QueryTimeout=0;TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys, INFORMATION_SCHEMA;};Catalog=DRILL;Schema=;ConnectionType=ZooKeeper;ZKQuorum=192.168.39.43:5181;ZKClusterID=drillbits1

#### What's Next? Go to [Step 3. Connect to Drill Data Sources from a BI Tool](/docs/step-3-connect-to-drill-data-sources-from-a-bi-tool).

