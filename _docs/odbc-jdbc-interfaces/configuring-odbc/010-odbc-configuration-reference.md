---
title: "ODBC Configuration Reference"
date: 2017-08-17 04:49:30 UTC
parent: "Configuring ODBC"
---
You can use various configuration options to control the behavior of the Drill ODBC Driver. You can use these options in a connection string or in the `odbc.ini` configuration file for the Mac OS X version or the driver.

{% include startnote.html %}If you use a connection string to connect to your data source, set these configuration properties in the connection string instead of the .odbc.ini file.{% include endnote.html %}

## Configuration Options

The following table provides a list of the configuration options and a brief description. Subsequent sections describe options in more detail:  

| ﻿Property | Default Values | Brief Description |
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| AdvancedProperties | `CastAnyToVarchar=true; HandshakeTimeout=5; QueryTimeout=180; TimestampTZDisplayTimezone=local; ExcludedSchemas= sys,INFORMATION_SCHEMA; NumberOfPrefetchBuffers=5` | Not required. Advanced properties for configuring the driver. You can set custom connection properties by specifying them as advanced properties.   If you specify a property that the driver does not explicitly support, the driver still accepts the property, and passes it to the server for processing.  Separate advanced properties using a semi-colon (;) and then surround all advanced properties in a connection string using braces { and }. For example,  `{<property>;<property>; . . .}`  In addition, the following Advanced Properties string excludes the schemas named `test` and `abc`, sets the timeout to 30 seconds, and sets the time zone to Coordinated Universal Time:`HandshakeTimeout=30;QueryTimeout=30;TimestampTZDisplayTimezone=utc;ExcludedSchemas=test,abc`. |
| AuthenticationType | No Authentication | Not required.  This option specifies how the driver authenticates the connection to Drill.   No Authentication: The driver does not authenticate the connection to Drill. Kerberos: The driver authenticates the connection using the Kerberos protocol. Plain: The driver authenticates the connection using a user name and a password.  |
| Catalog | The default catalog name specified  in the driver's .did file (typically, DRILL). | Not required. The name of the synthetic catalog under which all of the schemas/databases are organized. This catalog name is used as the value for SQL_DATABASE_NAME or CURRENT CATALOG. |
| ConnectionType | Direct to Drillbit (Direct) | Required. This option specifies whether the driver connects to a single server or a ZooKeeper cluster. Direct to Drillbit (Direct): The driver connects to a single Drill server. ZooKeeper Quorum (ZooKeeper): The driver connects to a ZooKeeper cluster. |
| DelegationUID | none | Not required. If a value is specified for this setting, the driver delegates all operations against Drill to the specified user, rather than to the authenticated user for the connection. This option is applicable only when Plain authentication is enabled. |
| DisableAsync | Clear (0) | Not required. This option specifies whether the driver supports asynchronous queries.   Enabled (1): The driver does not support asynchronous queries. Disabled (0): The driver supports asynchronous queries. This option is not supported in connection strings or DSNs. Instead, it must be set as a driver-wide property in the mapr.drillodbc.ini file. Settings in that file apply to all connections that use the driver. |
| Driver | MapR Drill ODBC Driver on Windows machines or the absolute path of the driver shared object file when installed on a non-Windows machine | On Windows, the name of the installed driver (MapR Drill ODBC Driver). On other platforms, the name of the installed driver as specified in odbcinst.ini, or the absolute path of the driver shared object file. |
| Host | localhost | Required if the ConnectionType property is set to Direct to Drillbit. The IP address or host name of the Drill server. |
| KrbServiceHost | none | Required for Kerberos authentication. The fully qualified domain name of the Drill server host. |
| KrbServiceName | `map` (default) | Required for Kerberos authentication. The Kerberos service principal name of the Drill server. mapr is the default for the MapR Drill ODBC driver. |
| LogLevel | OFF (0) | Not required. Use this property to enable or disable logging in the driver and to specify the amount of detail included in log files. Only enable logging long enough to capture an issue. Logging decreases performance and can consume a large quantity of disk space.   This option is not supported in connection strings. To configure logging for the Windows driver, you must use the Logging Options dialog box. To configure logging for a non-Windows driver, you must use the mapr.drillodbc.ini file. |
| LogPath | none | Required if logging is enabled. The full path to the folder where the driver saves log files when logging is enabled. When logging is enabled, the driver produces two log files at the location that you specify in the LogPath property:  driver.log provides a log of driver activities, and  drillclient.log provides a log of Drill client activities.   This option is not supported in connection strings. To configure logging for the Windows driver, you must use the Logging Options dialog box. To configure logging for a non-Windows driver, you must use the mapr.drillodbc.ini file. |
| Port | 31010 | Required if the ConnectionType property is set to Direct to Drillbit. The TCP port that the Drill server uses to listen for client connections. Set the TCP port on which the Drill server is listening. |
| PWD | none | Required if AuthenticationType is Plain (also known as Basic Authentication). The password corresponding to the user name that you provided in the User field (the UID key). |
| Schema | none | Not required. The name of the database schema to use when a schema is not explicitly specified in a query. You can still issue queries on other schemas by explicitly specifying the schema in the query. |
| UID | none | Required if AuthenticationType is Plain (also known as Basic authentication). Set UID to a user name. |
| UseOnlySSPI (on Windows only) | Clear (0) | Not required. This option is available only in the Windows driver. This option specifies how the driver handles Kerberos authentication: either with the SSPI plugin or with MIT Kerberos.   Enabled (1): The driver handles Kerberos authentication by using the SSPI plugin instead of MIT Kerberos by default. Disabled (0): The driver uses MIT Kerberos to handle Kerberos authentication, and only uses the SSPI plugin if the GSSAPI library is not available. |
| ZKClusterID | drillbits1 | Required if the ConnectionType property is set to ZooKeeper Quorum. Set ZKClusterID to the name of the Drillbit cluster to use. |
| ZKQuorum | none | Required if the ConnectionType property is set to ZooKeeper. Set  ZKQuorum to indicate the server(s) in your ZooKeeper cluster. Separate multiple servers using a comma (,). For example, `<IP address>`,`<IP address>`. |


### Catalog

This value defaults to DRILL and should not be changed. The driver adds a synthetic catalog named DRILL under which all of the schemas and databases are organized. The driver maps the ODBC schema to the DRILL catalog.

### Connection Type

ODBC can connect directly to a Drillbit or to a ZooKeeper Quorum. Select your
connection type based on your environment and Drillbit configuration as described in the following table:

| Environment                                                            | Connection Type                        |
|------------------------------------------------------------------------|----------------------------------------|
| Drillbit is running in embedded mode.                                  | Direct to Drillbit                     |
| Drillbit is registered with the ZooKeeper in a testing environment.    | ZooKeeper Quorum or Direct to Drillbit |
| Drillbit is registered with the ZooKeeper in a production environment. | ZooKeeper Quorum                       |

## Host Name and Port Number
When using ZooKeeper to connect to Drill, do not use the IP address in the connection string. Make sure the client system can resolve the actual hostname(s) by pinging the hostnames first. 

## ZKClusterID and ZKQuorum
The default cluster ID is drillbits1. Check the `drill-override.conf` in the Drill installation `/conf` directory. Use the cluster-id and zk.connect values for ZKClusterID and ZKQuorum.

### Connection to Zookeeper Quorum

When you choose to connect to a ZooKeeper Quorum, the ODBC driver connects to the ZooKeeper Quorum to get a list of available Drillbits in the specified cluster. Then, the ODBC driver submits a query after selecting a Drillbit. All Drillbits in the cluster process the query and the Drillbit that received the query returns the query results.

![ODBC to Quorum]({{ site.baseurl }}/docs/img/ODBC_to_Quorum.png)

In a production environment, you should connect to a ZooKeeper Quorum for a more reliable connection. If one Drillbit is not available, another Drillbit that is registered with the ZooKeeper quorum can accept the query.

### Direct Connection to Drillbit

When you choose to connect directly to a Drillbit, the ODBC driver connects to the Drillbit and submits a query. If you connect directly to Drillbit that is not part of a cluster, the Drillbit that you connect to processes the query. If you connect directly to a Drillbit that is part of a cluster, all Drillbits in the cluster process the query. In either case, the Drillbit that the ODBC driver connected to returns the query results.

![]({{ site.baseurl }}/docs/img/ODBC_to_Drillbit.png)

### Schema

The name of a schema, or [storage plugin]({ site.baseurl }}/docs/storage-plugin-registration/), from the default schema list of the data sources that you have configured to
use with Drill. Queries on other schemas can still be issued by explicitly specifying the schema in the query.

Views that you create using the Drill Explorer do not appear under the schema associated with the data source type. Instead, the views can be accessed from the file-based schema that you selected when saving the view.

The driver supports the following schema types:

* HBase
* Distributed File System (DFS), supporting the following file formats:
  * Parquet  
  * JSON
  * CSV
  * TSV
* Hive

### Advanced Properties

The Advanced Properties field allows you to customize the DSN. Separate advanced properties using a semi-colon (;).

For example, the following Advanced Properties string excludes the schemas named `test` and `abc`; sets the timeout to 30 seconds; and sets the time zone to Coordinated Universal:

`Time:HandshakeTimeout=30;QueryTimeout=30;
TimestampTZDisplayTimezone=utc;ExcludedSchemas=test,abc`

The following table lists and describes the advanced properties that you can set when using the MapR Drill ODBC Driver.

| ﻿Property                   | Default Values         | Brief Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|----------------------------|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CastAnyToVarchar           | True                   | Not required. When this property is set to true, if SQLColumns returns columns of type ANY, then the driver casts the columns to VARCHAR. When this property is set to false, the driver does not change the returned columns.                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ConvertToCast              | False                  | Not required. When activated this enables the Cast Query Translation function. This function helps optimize your queries for Power BI.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| DefaultDecimalScale        | 10                     | The default scale for DECIMAL columns that are returned through SQLColumns. The driver uses this value only if SQLColumns does not return a scale value or a numeric precision value that the driver can use to determine the scale.                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ExcludedSchemas            | sys,INFORMATION_SCHEMA | Not required. A list of schemas that do not appear in client applications such as Drill Explorer, Tableau, and Excel. Separate schemas in the list using a comma (,).This property should not be used at the same time as IncludedSchemas. If both IncludedSchemas and ExcludedSchemas are specified, IncludedSchemas takes precedence and ExcludedSchemas is ignored.                                                                                                                                                                                                                                                                                                                       |
| GetMetadataWithQueries     | none                   | Not required. This property specifies whether the driver uses queries or native API calls when retrieving metadata, preparing statements, or executing statements. The driver can only use native API calls if it detects that the server is running a version of Drill that supports those calls. If this property is set to True, the driver uses queries to retrieve metadata, prepare statements, and execute statements. l If this property is set to False, the driver instead uses native API calls when it is connected to a server that supports native API calls.                                                                                                                   |
| HandshakeTimeout           | 5                      | Not required. An integer value representing the number of seconds that the driver waits before stopping an attempt to connect to a data source. If this property is set to 0, the driver does not stop connection attempts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| HeartBeatFreqSec           | 15                     | Not required. The number of seconds of inactivity before the Drill client sends a heartbeat to the server in order to check the status of the connection. If this property is set to 0, the Drill client does not send any heartbeats.                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| IncludedSchemas            | none                   | Not required. A list of schemas that appear in client applications such as Drill Explorer, Tableau, and Excel. Separate schemas in the list using a comma (,). If this option is set, then schemas that are not in this list are not queried by the driver.                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| NonZeroNameMaxLen          | False                  | Not required. The MapR Drill ODBC Driver does not enforce a maximum length for schema, catalog, table, and column names. By default, the driver reports these lengths as 0, which typically indicates that there is no maximum length. However, some BI tools interpret a length of 0 literally. To make sure that the names are displayed correctly in your BI tool,set the NonZeroNameMaxLen property. When this property is set to true, the driver reports an appropriate non-zero value for the maximum length of schema, catalog, table, and column names. When this property is set to false, the driver reports 0 as the maximum length of schema, catalog, table, and column names. |
| NumberOfPrefetchBuffers    | 5                      | Not required. The size of the record batch queue in the driver. When set to a value below 1, the value defaults to 1.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| QueryTimeout               | 180                    | Not required. The number of seconds that the driver waits before stopping a query. If this property is set to 0, the driver does not stop queries.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| RemoveQryCatalog           | True                   | Not required. When this property is set to true, the driver removes catalog names from queries. Enable this option if you are working with a server that does not accept queries containing catalog names. When this property is set to false, the driver does not remove catalog names before executing queries.                                                                                                                                                                                                                                                                                                                                                                            |
| SaslPluginDir              | none                   | Not required. Allows you to override the default SASL plugin directory with a custom directory path.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| StringColumnLength         | 1024                   | Not required. The maximum column length that the driver reports for string columns.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| TimestampTZDisplayTimezone | local                  | Not required. Two values are possible: local and utc. When set to local, timestamps use the time zone of the user. When set to utc, timestamps appear in Coordinated Universal Time (UTC).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

### Connection String Examples

If you want to connect to a Drill data source from an application that does
not require a DSN, you can use an ODBC connection string. The following is an example connection string for the Direct connection type:  

    DRIVER=MapR Drill ODBC Driver;AdvancedProperties={HandshakeTimeout=0;QueryTimeout=0;TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys,INFORMATION_SCHEMA;};Catalog=DRILL;Schema=hivestg;ConnectionType=Direct;Host=192.168.202.147;Port=31010

The following is an example connection string for the Zookeeper connection type:  

    DRIVER=MapR Drill ODBC Driver;AdvancedProperties={HandshakeTimeout=0;QueryTimeout=0;TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys, INFORMATION_SCHEMA;};Catalog=DRILL;Schema=;ConnectionType=ZooKeeper;ZKQuorum=192.168.39.43:5181;ZKClusterID=drillbits1






