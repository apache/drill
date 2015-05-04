---
title: "Starting/Stopping Drill"
parent: "Manage Drill"
---
How you start Drill depends on the installation method you followed. If you installed Drill in embedded mode, invoking SQLLine automatically starts a Drillbit locally. If you installed Drill in distributed mode on a MapR cluster, the installation process starts the Drillbit service automatically; otherwise,  you must start the Drillbit service before invoking running queries.

## Controlling a Drillbit

Using the **drillbit command**, located in the `bin` directory, check the status of, start, start, and restart a DrillBit. You can use a configuration file to start Drill. Using such a file is handy for controlling Drillbits on multiple nodes.

### drillbit Command Syntax

    drillbit.sh [--config <conf-dir>] (start|stop|status|restart|autorestart)

For example, to restart a Drillbit, navigate to the Drill installation directory, and issue the following command:

    bin/drillbit.sh restart

## Invoking SQLLine
SQLLine is used as the Drill shell. SQLLine connects to relational databases and executes SQL commands. You invoke SQLLine for Drill in embedded or distributed mode. If you want to use a particular storage plugin, you can indicate the schema name when you invoke SQLLine.
To start SQLLine, use the following **sqlline command** syntax:

### SQLLine Command Syntax

    sqlline –u jdbc:drill:[schema=<storage plugin>;]zk=<zk name>[:<port>][,<zk name2>[:<port>]... ]

#### sqlline Arguments 

* `-u` is the option that precedes a connection string. Required.  
* `jdbc` is the connection protocol. Required.  
* `schema` is the name of a storage plugin to use for queries. Optional.  
* `Zk=zkname` is one or more zookeeper host names or IP addresses. Required.  
* `port` is the zookeeper port number. Optional. Port 2181 is the default.  

## Examples of Starting Drill
Issue the sqlline command from the Drill installation directory. For example, this command starts Drill on an embedded mode (single-node) cluster:

    bin/sqlline –u jdbc:drill:schema=dfs;zk=localhost

This command starts Drill on a distributed mode (multi-node) cluster configured to run zookeeper on three nodes:

    bin/sqlline –u jdbc:drill:zk=cento23,zk=centos24,zk=centos26:5181

## Exiting SQLLine

To exit SQLLine, issue the following command:

    !quit  

