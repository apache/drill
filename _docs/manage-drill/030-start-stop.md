---
title: "Starting/Stopping Drill"
parent: "Manage Drill"
---
How you start Drill depends on the installation method you followed. If you installed Drill in embedded mode, invoking SQLLine automatically starts a Drillbit locally. 

On a MapR cluster, Drill runs as a service and the installation process starts the Drillbit service automatically. If you installed Drill in distributed mode, and the Drillbit on a node did not start, start the Drillbit before attempting to run queries.


## Controlling a Drillbit

The Drillbit service accepts requests from the client, processing the queries, and returning results to the client. You install Drill as a service and run the Drillbit on all of the required nodes in a Hadoop cluster to form a distributed cluster environment. When a Drillbit runs on each data node in the cluster, Drill maximizes data locality during query execution. Movement of data over the network or between nodes is minimized or eliminated when possible.

If you use Drill in distributed mode, you need to understand how to control a Drillbit. If you use Drill in embedded mode, you do not use the **drillbit** command. Windows and Mac OS X run Drill only in embedded mode, and therefore do not use the Drillbit command.

Using the **drillbit command**, located in the `bin` directory, you check the status of the Drillbit, start, stop, and restart a DrillBit. You can use a configuration file to start Drill. Using such a file is handy for controlling Drillbits on multiple nodes.

### drillbit Command Syntax

    drillbit.sh [--config <conf-dir>] (start|stop|status|restart|autorestart)

For example, to restart a Drillbit, navigate to the Drill installation directory, and issue the following command:

    bin/drillbit.sh restart

## Invoking SQLLine
SQLLine is used as the Drill shell. SQLLine connects to relational databases and executes SQL commands. You invoke SQLLine for Drill in embedded or distributed mode. If you want to use a particular storage plugin, you specify the plugin as a schema when you invoke SQLLine.

### SQLLine Command Syntax
To start SQLLine, use the following **sqlline command** syntax:


    sqlline –u jdbc:drill:[schema=<storage plugin>;]zk=<zk name>[:<port>][,<zk name2>[:<port>]... ]

#### sqlline Arguments 

* `-u` is the option that precedes a connection string. Required.  
* `jdbc` is the connection protocol. Required.  
* `schema` is the name of a [storage plugin]({{site.baseurl}}/docs/storage-plugin-registration) to use for queries. Optional.  
* `Zk=zkname` is one or more zookeeper host names or IP addresses, or the keyword `local`, which is an alias for localhost. Required.  
* `port` is the zookeeper port number. Optional. Port 2181 is the default.  

## Examples of Starting Drill
Issue the **sqlline** command from the Drill installation directory. The simplest example of how to start SQLLine is to identify the protocol, JDBC, and zookeeper node or nodes in the **sqlline** command. This example starts SQLLine on a node in an embedded, single-node cluster:

    sqlline -u jdbc:drill:zk=local

This example also starts SQLLine in embedded mode using the `dfs` storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query:


    bin/sqlline –u jdbc:drill:schema=dfs;zk=localhost

This command starts SQLLine in distributed, (multi-node) mode in a cluster configured to run zookeeper on three nodes:

    bin/sqlline –u jdbc:drill:zk=cento23,zk=centos24,zk=centos26:5181

## Exiting SQLLine

To exit SQLLine, issue the following command:

    !quit

## Stopping Drill

In some cases, such as stopping while a query is in progress, the `!quit` command does not stop Drill running in embedded mode. In distributed mode, you stop the Drillbit service instead of killing the Drillbit process. 

To stop the Drill process on Mac OS X and Linux, use the kill command. On Windows, use the **TaskKill** command.

For example, on Mac OS X and Linux, follow these steps:

  1. Issue a CTRL Z to stop the query, then start Drill again. If the startup message indicates success, skip the rest of the steps. If not, proceed to step 2.
  2. Search for the Drill process IDs.
  
        $ ps auwx | grep drill
  3. Kill each process using the process numbers in the grep output. For example:

        $ sudo kill -9 2674  

