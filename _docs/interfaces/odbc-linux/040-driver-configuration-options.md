---
title: "Driver Configuration Options"
parent: "Using ODBC on Linux and Mac OS X"
---
You can use various configuration options to control the behavior of the MapR
Drill ODBC Driver on Linux and Mac OS X. You can use these options in a connection string or in the
`odbc.ini` configuration file for the Mac OS X version or the driver.

**Note:** If you use a connection string to connect to your data source, then you can set these configuration properties in the connection string instead of the` odbc.ini` file.

The following table provides a list of the configuration options with their
descriptions:

<table ><tbody><tr><td valign="top"><strong>Property Name</strong></td><td valign="top"><strong>Description</strong></td></tr><tr><td valign="top">AdvancedProperties</td><td valign="top">Separate advanced properties using a semi-colon (;) and then surround all advanced properties in a connection string using braces { and }. <br />For details on supported advanced properties, see Appendix C: Advanced Properties.</td></tr><tr><td valign="top">Catalog</td><td valign="top">The name of the synthetic catalog under which all of the schemas/databases are organized: DRILL</td></tr><tr><td valign="top">ConnectionType</td><td valign="top">The following values are possible:<ul><li>Direct - Connect to a Drill server using Host and Port properties in the connection string.</li><li>ZooKeeper - Connect to a ZooKeeper cluster using ZKQuorum and ZKClusterID properties in the connection string.</li></ul></td></tr><tr><td valign="top">DRIVER</td><td valign="top">(Required) The name of the installed driver: MapR Drill ODBC Driver </td></tr><tr><td valign="top">Host</td><td valign="top">If the ConnectionType property is set to Direct, then indicate the IP address or hostname of the Drill server using the Host property.</td></tr><tr><td valign="top">Port</td><td valign="top">If the ConnectionType property is set to Direct, then indicate the port on which the Drill server is listening using the Port property.</td></tr><tr><td valign="top">Schema</td><td valign="top">The name of the database schema to use when a schema is not explicitly specified in a query.Note: Queries on other schemas can still be issued by explicitly specifying the schema in the query.</td></tr><tr><td valign="top">ZKClusterID</td><td valign="top">If the ConnectionType property is set to ZooKeeper, then use ZKClusterID to indicate the name of the Drillbit cluster to use.</td></tr><tr><td valign="top">ZKQuorum</td><td valign="top">If the ConnectionType property is set to ZooKeeper, then use ZKQuorum to indicate the server(s) in your ZooKeeper cluster. Separate multiple servers using a comma (,).</td></tr></tbody></table>

