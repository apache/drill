---
title: Starting Drill in Distributed Mode
date: TBD 
parent: "Installing Drill in Distributed Mode"
---

You start Drill in distributed mode in one of the following ways:

* Using the **drillbit.sh** command
* Starting the Drill Shell
* Using an Ad-Hoc Connection to Drill

## Using the drillbit.sh Command
To use Drill in distributed mode, you need to control a Drillbit. If you use Drill in embedded mode, you do not use the **drillbit.sh** command. 

Using the **drillbit.sh** command you can perform the following tasks:

* Check the status of the Drillbit
* Start, stop, or restart a Drillbit
* Configure a Drillbit to restart by default

You can use a configuration file to start Drill. Using such a file is handy for controlling Drillbits on multiple nodes.

### drillbit.sh Command Syntax

`drillbit.sh [--config <conf-dir>] (start|stop|status|restart|autorestart)`

For example, to restart a Drillbit on a tarball installation, navigate to the Drill installation directory, and issue the following command from the installation directory:

`bin/drillbit.sh restart`

## Starting the Drill Shell
Using the Drill shell, you can connect to data sources and execute SQL commands. To start the Drill shell, run one of the following scripts, which are located in the bin directory of the Drill installation:

* `drill-conf`  
  Opens the Drill shell using the connection string to ZooKeeper nodes specified in `drill-override.conf` in `<installation directory>/conf`.  
* `drill-localhost`  
  Opens the Drill shell using a connection to the ZooKeeper running on the local host.

The [Drill prompt]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/#about-the-drill-prompt) appears when you start the Drill shell.

Issue the following query to check the Drillbits running in the cluster:
`0: jdbc:drill:zk=<zk1host>:<port> SELECT * FROM sys.drillbits;`  
Drill lists information about the Drillbits that are running, such as the host name and data port number.

## Using an Ad-Hoc Connection to Drill
To use a custom connection to Drill, but not alter the connection string in `drill-conf` that you normally use, start the Drill shell on an ad-hoc basis using `sqlline`. For example, to start the Drill shell using a particular storage plugin as a schema, use the following command syntax: 

`sqlline –u jdbc:drill:[schema=<storage plugin>;]zk=<zk name>[:<port>][,<zk name2>[:<port>]... ]`

### sqlline Arguments and Connection Parameters

* `-u` is the option that precedes a connection string. Required.  
* `jdbc` is the connection type. Required.  
* `schema` is the name of a [storage plugin]({{site.baseurl}}/docs/storage-plugin-registration) configuration to use as the default for queries. Optional.  
* `zk name` specifies one or more ZooKeeper host names or IP addresses.  
* `port` is the ZooKeeper port number. Optional. Port 2181 is the default for the open source version of Apache Drill.  

For example, start the Drill shell with the default schema being the `dfs` storage plugin:

`bin/sqlline –u jdbc:drill:schema=dfs;zk=centos26`

Specifying the storage plugin configuration name when you start up eliminates the need to enter `USE <schema name>` or specify the it in the FROM clause.

The following command starts the Drill shell in a cluster configured to run ZooKeeper on three nodes:

`bin/sqlline –u jdbc:drill:zk=cento23,zk=centos24,zk=centos26:5181`

## Making a Direct Drillbit Connection

If you want to connect directly to a Drillbit instead of using ZooKeeper to choose the Drillbit, replace `zk=<zk name>` with `drillbit=<node>` as shown in the following URL.

`jdbc:drill:[schema=<storage plugin>;]drillbit=<node name>[:<port>][,<node name2>[:<port>]... `  
  `]<directory>/<cluster ID>`

where

`drillbit=<node name>` specifies one or more host names or IP addresses of cluster nodes running Drill. 

## Exiting the Drill Shell

To exit the Drill shell, issue the following command:

`!quit`

## Stopping Drill
Navigate to the Drill installation directory, and issue the following command to stop a Drillbit:
  
`bin/drillbit.sh stop`
