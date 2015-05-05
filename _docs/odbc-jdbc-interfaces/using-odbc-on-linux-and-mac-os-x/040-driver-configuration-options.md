---
title: "Driver Configuration Options"
parent: "Using ODBC on Linux and Mac OS X"
---
You can use various configuration options to control the behavior of the MapR
Drill ODBC Driver on Linux and Mac OS X. You can use these options in a connection string or in the
`odbc.ini` configuration file for the Mac OS X version or the driver.

{% include startnote.html %}If you use a connection string to connect to your data source, then you can set these configuration properties in the connection string instead of the` odbc.ini` file.{% include endnote.html %}

The following table provides a list of the configuration options with their
descriptions:

| Property Name      | Description                                                                                                                                                                                                                                                                                               |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| AdvancedProperties | Separate advanced properties using a semi-colon (;) and then surround all advanced properties in a connection string using braces { and }. For details on supported advanced properties, see Appendix C: Advanced Properties.                                                                             |
| Catalog            | The name of the synthetic catalog under which all of the schemas/databases are organized: DRILL                                                                                                                                                                                                           |
| ConnectionType     | The following values are possible: Direct and Zookeeper. The Direct connection type connects to a Drill server using Host and Port properties in the connection string. The Zookeeper connection type connects to a Zookeeper cluster using ZKQuorum and ZKClusterID properties in the connection string. |
| DRIVER             | (Required) The name of the installed driver: MapR Drill ODBC Driver                                                                                                                                                                                                                                       |
| Host               | If the ConnectionType property is set to Direct, then indicate the IP address or hostname of the Drill server using the Host property.                                                                                                                                                                    |
| Port               | If the ConnectionType property is set to Direct, then indicate the port on which the Drill server is listening using the Port property.                                                                                                                                                                   |
| Schema             | The name of the database schema to use when a schema is not explicitly specified in a query.Note: Queries on other schemas can still be issued by explicitly specifying the schema in the query.                                                                                                          |
| ZKClusterID        | If the ConnectionType property is set to ZooKeeper, then use ZKClusterID to indicate the name of the Drillbit cluster to use.                                                                                                                                                                             |
| ZKQuorum           | If the ConnectionType property is set to ZooKeeper, then use ZKQuorum to indicate the server(s) in your ZooKeeper cluster. Separate multiple servers using a comma (,).                                                                                                                                   |

