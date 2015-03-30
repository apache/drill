---
title: "Configuring ODBC Connections for Linux and Mac OS X"
parent: "Using the MapR ODBC Driver on Linux and Mac OS X"
---
You can use a connection string to connect to your data source. For a list of
all the properties that you can use in connection strings, see [Driver
Configuration
Options](/docs/driver-configuration-options).

The following example shows a connection string for connecting directly to a
Drillbit:

**Example**

    DRIVER=MapR Drill ODBC Driver;AdvancedProperties= {HandshakeTimeout=0;QueryTimeout=0; TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys, INFORMATION_SCHEMA;[OS1] };Catalog=DRILL;Schema=hivestg; ConnectionType=Direct;Host=192.168.202.147;Port=31010

The following example shows a connection string for connecting to a ZooKeeper
cluster:

**Example**

    DRIVER=MapR Drill ODBC Driver;AdvancedProperties= {HandshakeTimeout=0;QueryTimeout=0; TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys, INFORMATION_SCHEMA;};Catalog=DRILL;Schema=; ConnectionType=ZooKeeper;ZKQuorum=192.168.39.43:5181; ZKClusterID=drillbits1

