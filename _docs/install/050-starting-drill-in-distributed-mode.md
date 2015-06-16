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

## Starting the Drill Shell
Using the Drill shell, you can connect to relational databases and execute SQL commands. To start the Drill shell, run one of the following scripts, which are located in the bin directory of the Drill installation:

* `drill-conf`  
  Opens the shell using the connection string to ZooKeeper nodes specified in `drill-override.conf` in `/opt/mapr/drill/drill-1.0.0/conf`.  
* `drill-localhost`  
  Opens the Drill shell using a connection to the ZooKeeper running on the local host.

Complete the following steps to start the Drill shell on the local node:

  1. Navigate to the Drill installation directory, and issue the following command to start the Drillbit if necessary:
  
        bin/drillbit.sh restart
  2. Issue the following command to start the Drill shell if ZooKeeper is running on the same node as the shell:
  
        bin/drill-localhost
     
     Alternatively, issue the following command to start the Drill shell using the connection string in `drill-conf`:

         bin/drill-conf

  3. Issue the following query to check the Drillbits running in the cluster:
  
        0: jdbc:drill:zk=<zk1host>:<port> select * from sys.drillbits;

Drill provides a list of Drillbits that are running.

    +----------------+---------------+---------------+---------------+-----------+
    |    hostname    | user_port     | control_port  |   data_port   |  current  |
    +----------------+---------------+---------------+---------------+-----------+
    | <host address> | <port number> | <port number> | <port number> | <boolean> |
    +----------------+---------------+---------------+---------------+-----------+

Now you can run queries. The Drill installation includes sample data
that you can query. Refer to [Querying Parquet Files]({{ site.baseurl }}/docs/querying-parquet-files/).

### Using an Ad-Hoc Connection to Drill
To use a custom connection to Drill, but not alter the connection string in `drill-conf` that you normally use, start the Drill shell on an ad-hoc basis using `sqlline`. For example, to start the Drill shell using a particular storage plugin as a schema, use the following command syntax: 

    sqlline –u jdbc:drill:[schema=<storage plugin>;]zk=<zk name>[:<port>][,<zk name2>[:<port>]... ]

#### sqlline Arguments 

* `-u` is the option that precedes a connection string. Required.  
* `jdbc` is the connection protocol. Required.  
* `schema` is the name of a [storage plugin]({{site.baseurl}}/docs/storage-plugin-registration) to use for queries. Optional.  
* `Zk=zkname` is one or more ZooKeeper host names or IP addresses.  
* `port` is the ZooKeeper port number. Optional. Port 2181 is the default.  

For example, start the Drill shell using the `dfs` storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query:

    bin/sqlline –u jdbc:drill:schema=dfs;zk=centos26

This command starts the Drill shell in a cluster configured to run ZooKeeper on three nodes:

    bin/sqlline –u jdbc:drill:zk=cento23,zk=centos24,zk=centos26:5181

## Exiting the Drill Shell

To exit the Drill shell, issue the following command:

    !quit

## Stopping Drill

In some cases, such as stopping while a query is in progress, the `!quit` command does not stop Drill running in embedded mode. In distributed mode, you stop the Drillbit service. Navigate to the Drill installation directory, and issue the following command to stop a Drillbit:
  
    bin/drillbit.sh stop
