---
title: Starting Drill in Distributed Mode
parent: "Installing Drill in Distributed Mode"
---

The Drillbit service accepts requests from the client, processing the queries, and returning results to the client. You install Drill as a service and run the Drillbit on all of the required nodes in a Hadoop cluster to form a distributed cluster environment. When a Drillbit runs on each data node in the cluster, Drill maximizes data locality during query execution. Movement of data over the network or between nodes is minimized or eliminated when possible.

To use Drill in distributed mode, you need to control a Drillbit. If you use Drill in embedded mode, you do not use the **drillbit** command. 

Using the **drillbit command**, located in the `bin` directory, you check the status of the Drillbit, start, stop, and restart a DrillBit. You can use a configuration file to start Drill. Using such a file is handy for controlling Drillbits on multiple nodes.

### drillbit Command Syntax

    drillbit.sh [--config <conf-dir>] (start|stop|status|restart|autorestart)

For example, to restart a Drillbit, navigate to the Drill installation directory, and issue the following command:

    bin/drillbit.sh restart

## Invoking SQLLine
SQLLine is used as the Drill shell. SQLLine connects to relational databases and executes SQL commands. You invoke SQLLine for Drill in embedded or distributed mode. If you want to use a particular storage plugin, you specify the plugin as a schema when you invoke SQLLine.

### SQLLine Command Syntax on Linux and Mac OS X
To start SQLLine, use the following **sqlline command** syntax:


    sqlline –u jdbc:drill:[schema=<storage plugin>;]zk=<zk name>[:<port>][,<zk name2>[:<port>]... ]

#### sqlline Arguments 

* `-u` is the option that precedes a connection string. Required.  
* `jdbc` is the connection protocol. Required.  
* `schema` is the name of a [storage plugin]({{site.baseurl}}/docs/storage-plugin-registration) to use for queries. Optional.  
* `Zk=zkname` is one or more zookeeper host names or IP addresses. Optional if you are running SQLLine and zookeeper on the local node.  
* `port` is the zookeeper port number. Optional. Port 2181 is the default.  

#### Examples of Starting Drill
This example also starts SQLLine using the `dfs` storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query:


    bin/sqlline –u jdbc:drill:schema=dfs;zk=centos26

This command starts SQLLine in a cluster configured to run zookeeper on three nodes:

    bin/sqlline –u jdbc:drill:zk=cento23,zk=centos24,zk=centos26:5181

## Procedure for Starting Drill in Distributed Mode

Complete the following steps to start Drill:

  1. Navigate to the Drill installation directory, and issue the following command to start a Drillbit:
  
        bin/drillbit.sh restart
  2. Issue the following command to invoke SQLLine and start Drill if zookeeper is running on the same node as SQLLine:
  
        bin/sqlline -u jdbc:drill:
     
     If you cannot connect to Drill, invoke SQLLine with the ZooKeeper quorum:

         bin/sqlline -u jdbc:drill:zk=<zk1host>:<port>,<zk2host>:<port>,<zk3host>:<port>
  3. Issue the following query to Drill to verify that all Drillbits have joined the cluster:
  
        0: jdbc:drill:zk=<zk1host>:<port> select * from sys.drillbits;

Drill provides a list of Drillbits that have joined.

    +------------+------------+--------------+--------------------+
    |    host        | user_port    | control_port | data_port    |
    +------------+------------+--------------+--------------------+
    | <host address> | <port number>| <port number>| <port number>|
    +------------+------------+--------------+--------------------+

Now you can run queries. The Drill installation includes sample data
that you can query. Refer to [Querying Parquet Files]({{ site.baseurl }}/docs/querying-parquet-files/).