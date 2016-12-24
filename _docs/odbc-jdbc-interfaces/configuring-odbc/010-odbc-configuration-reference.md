---
title: "ODBC Configuration Reference"
date: 2016-12-24 00:32:37 UTC
parent: "Configuring ODBC"
---

You can use various configuration options to control the behavior of the MapR
Drill ODBC Driver. You can use these options in a connection string or in the
`odbc.ini` configuration file for the Mac OS X version or the driver.

{% include startnote.html %}If you use a connection string to connect to your data source, set these configuration properties in the connection string instead of the .odbc.ini file.{% include endnote.html %}

## Configuration Options

The following table provides a list of the configuration options and a brief description. Subsequent sections describe options in more detail:  

| Property           | Valid Values                                | Brief Description                                                                                                                                                             |
|--------------------|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Catalog            | DRILL                                       | The   name of the synthetic catalog under which all of the schemas/databases are   organized.                                                                                 |
| ConnectionType     | Direct   or ZooKeeper                       | Direct   connects to a Drill server using Host and Port properties. ZooKeeper connects   to a ZooKeeper cluster using ZKQuorum and ZKClusterID properties.                    |
| Driver             | MapR   Drill ODBC Driver                    | The   name of the installed driver.                                                                                                                                           |
| Host               | `<host   name>`                               | If   the ConnectionType property is set to Direct, set the host name of the Drill   server using the Host property.                                                           |
| Port               | 31010                                       | If   the ConnectionType property is set to Direct, set the e TCP port on which the   Drill server is listening.                                                               |
| Schema             | `<schema   name>`                             | The   name of the database schema or storage plugin name to use when the query does   not explicitly specify the schema or storage plugin.                                    |
| ZKClusterID        | drillbits1                                  | If   the ConnectionType property is set to ZooKeeper, then set ZKClusterID to the   name of the Drillbit cluster to use.                                                      |
| ZKQuorum           | `<IP   address>`,`<IP address>` . . .           | If   the ConnectionType property is set to ZooKeeper, then use ZKQuorum to   indicate the server(s) in your ZooKeeper cluster. Separate multiple servers   using a comma (,). |
| AuthenticationType | No   Authentication or Basic Authentication | Basic Authentication   [enables impersonation](https://drill.apache.org/docs/configuring-user-impersonation/).                                                                                                                                 |
| UID                | `<user   name>`                               | If   AuthenticationType is Basic Authentication, set the UID to a user name.                                                                                                  |
| PWD                | `<password>`                                  | If   AuthenticationType is Basic Authentication, set the PWD to a password.                                                                                                   |
| DelegationUID      | `<impersonation_target>`                      | The impersonation target for the   authorized proxy user. See [Configuring Inbound Impersonation](https://drill.apache.org/docs/configuring-inbound-impersonation/).                                                                              |
| AdvancedProperties | {`<property>`;`<property>`;   . . .}            | Separate   advanced properties using a semi-colon (;) and then surround all advanced   properties in a connection string using braces { and }.                                |
| DisableAsync       | 0   or 1                                    | Disables   asynchronous ODBC connection and enables a synchronous connection. A change   in state occurs during driver initialization and is propagated to all driver   DSNs. |  


### Catalog

This value defaults to DRILL and cannot be changed. The driver adds a synthetic catalog named DRILL under which all of the schemas and databases are organized. The driver maps the ODBC schema to the DRILL catalog.

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

When you choose to connect to a ZooKeeper Quorum, the ODBC driver connects to
the ZooKeeper Quorum to get a list of available Drillbits in the specified
cluster. Then, the ODBC driver submits a query after selecting a Drillbit. All
Drillbits in the cluster process the query and the Drillbit that received the
query returns the query results.

![ODBC to Quorum]({{ site.baseurl }}/docs/img/ODBC_to_Quorum.png)

In a production environment, you should connect to a ZooKeeper Quorum for a
more reliable connection. If one Drillbit is not available, another Drillbit
that is registered with the ZooKeeper quorum can accept the query.

### Direct Connection to Drillbit

When you choose to connect directly to a Drillbit, the ODBC driver connects to
the Drillbit and submits a query. If you connect directly to Drillbit that is
not part of a cluster, the Drillbit that you connect to processes the query.
If you connect directly to a Drillbit that is part of a cluster, all Drillbits
in the cluster process the query. In either case, the Drillbit that the ODBC
driver connected to returns the query results.

![]({{ site.baseurl }}/docs/img/ODBC_to_Drillbit.png)

### Schema

The name of a schema, or [storage plugin]({ site.baseurl }}/docs/storage-plugin-registration/), from the default schema list of the data sources that you have configured to
use with Drill. Queries on other schemas can still be issued by explicitly specifying the schema in the query.

Views that you create using the Drill Explorer do not appear under the schema
associated with the data source type. Instead, the views can be accessed from
the file-based schema that you selected when saving the view.

The driver supports the following schema types:

* HBase
* Distributed File System (DFS), supporting the following file formats:
  * Parquet  
  * JSON
  * CSV
  * TSV
* Hive

### Advanced Properties

The Advanced Properties field allows you to customize the DSN.  
Separate advanced properties using a semi-colon (;).

For example, the following Advanced Properties string excludes the schemas
named `test` and `abc`; sets the timeout to 30 seconds; and sets the time zone
to Coordinated Universal:

`Time:HandshakeTimeout=30;QueryTimeout=30;
TimestampTZDisplayTimezone=utc;ExcludedSchemas=test,abc`

The following table lists and describes the advanced properties that you can
set when using the MapR Drill ODBC Driver.

| Property Name              | Default Value           | Description                                                                                                                                                                                                |
|----------------------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| HandshakeTimeout           | 5                       | An integer value representing the number of seconds that the driver waits before aborting an attempt to connect to a data source. When set to a value of 0, the driver does not abort connection attempts. |
| QueryTimeout               | 180                     | An integer value representing the number of seconds for the driver to wait before automatically stopping a query. When set to a value of 0, the driver does not stop queries automatically.                |
| TimestampTZDisplayTimezone | local                   | Two values are possible: local—Timestamps are dependent on the time zone of the user. utc—Timestamps appear in Coordinated Universal Time (UTC).                                                           |
| ExcludedSchemas            | sys, INFORMATION_SCHEMA | The value of ExcludedSchemas is a list of schemas that do not appear in client applications such as Drill Explorer, Tableau, and Excel. Separate schemas in the list using a comma (,).                    |
| CastAnyToVarchar           | true                    | Casts the ANY data type returned from SQL column calls into type “VARCHAR”.                                                                                                                                |
| NumberOfPrefetchBuffers    | 5                       | The size of the record batch queue in the driver. When set to a value below 1, the value defaults to 1.                                                                                                    |

### Connection String Examples

If you want to connect to a Drill data source from an application that does
not require a DSN, you can use an ODBC connection string. The following is an example connection string for the Direct connection type:  

        DRIVER=MapR Drill ODBC Driver;AdvancedProperties={HandshakeTimeout=0;QueryTimeout=0;TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys,INFORMATION_SCHEMA;};Catalog=DRILL;Schema=hivestg;ConnectionType=Direct;Host=192.168.202.147;Port=31010

The following is an example connection string for the Zookeeper connection
type:  

        DRIVER=MapR Drill ODBC Driver;AdvancedProperties={HandshakeTimeout=0;QueryTimeout=0;TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys, INFORMATION_SCHEMA;};Catalog=DRILL;Schema=;ConnectionType=ZooKeeper;ZKQuorum=192.168.39.43:5181;ZKClusterID=drillbits1

## Logging Options

Configure logging to troubleshoot issues. To configure logging, click the
Logging Options button on the ODBC DSN Setup dialog and then set a log level
and a log path.

If logging is enabled, the MapR Drill ODBC driver logs events in following log
files in the log path that you configure:

* driver.log: A log of driver events
* drillclient.log: A log of the Drill client events.


### Logging Levels

The following log levels are available:

* OFF: Disables logging.
* FATAL: Logs severe error events that may cause the driver to stop running.
* ERROR: Logs error events that may allow the driver to continue running.
* WARNING: Logs events about potentially harmful situations.
* INFO: Logs high-level events about driver processes.
* DEBUG: Logs detailed events that may help to debug issues.
* TRACE: Logs finer-grained events than the DEBUG level.

### Enabling Logging

To enable logging:

1. Open the mapr.drillodbc.ini configuration file in a text editor.  
2. Set the LogLevel key to the desired level of information to include in log files. For
example:  
`LogLevel=2`
3. Set the LogPath key to the full path to the folder where you want to save log files. For example:  
`LogPath=/localhome/employee/Documents`
4. Save the mapr.drillodbc.ini configuration file.  
The Simba ODBC Driver for Apache Drill produces two log files at the location you specify using the Log Path field:  
   * driver.log provides a log of driver activities
   * drillclient.log provides a log of Drill client activities

### Disabling Logging

To disable logging:

1. Open the mapr.drillodbc.ini configuration file in a text editor.
2. Set the LogLevel key to 0
3. Save the mapr.drillodbc.ini configuration file.

#### What's Next? Go to [Connecting to ODBC Data Sources]({{ site.baseurl }}/docs/connecting-to-odbc-data-sources).

