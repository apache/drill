---
title: "Starting Drill in Distributed Mode"
date: 2018-03-19 21:55:14 UTC
parent: "Installing Drill in Distributed Mode"
---

To use Drill in distributed mode, you first need to start a Drill daemon (Drillbit) on each node in the cluster. Start the Drillbit before attempting to connect a client. To start the Drillbit, use the **drillbit.sh** command.

{% include startnote.html %} If you use Drill in embedded mode, do not use the drillbit.sh command. {% include endnote.html %}

## Using the drillbit.sh Command
In addition to starting a Drillbit, you use the **drillbit.sh** command to perform the other tasks:

* Check the status of the Drillbit
* Stop or restart a Drillbit
* Configure a Drillbit to restart by default

You can use a configuration file to start Drillbits. A configuration file is useful for controlling Drillbits on multiple nodes.

### drillbit.sh Command Syntax

`drillbit.sh [--config <conf-dir>] (start|stop|graceful_stop|status|restart|autorestart)`

For example, to restart a Drillbit on a tarball installation, navigate to the Drill installation directory, and issue the following command from the installation directory:

`bin/drillbit.sh restart`

## Starting the Drill Shell

Using the Drill shell, you can interactively query data in connected data sources using SQL commands. To start the Drill shell, run one of the following scripts, which are located in the bin directory of the Drill installation:

* `drill-conf`  
  Opens the Drill shell using the connection string to ZooKeeper nodes specified in `drill-override.conf` in `<installation directory>/conf`.  
* `drill-localhost`  
  Opens the Drill shell using a connection to the ZooKeeper running on the local host.  

**Note:** As of Drill 1.12, users must enter a username to issue queries through curl commands if user impersonation is enabled and authentication is disabled. See [REST API]({{site.baseurl}}/docs/submitting-queries-from-the-rest-api-when-impersonation-is-enabled-and-authentication-is-disabled/) for more information.

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

`bin/sqlline –u jdbc:drill:zk=cento23,centos24,centos26:5181`

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

You can abruptly stop the Drill process on a node, or you can have the Drill process on the node shutdown gracefully. When you stop the Drill process on a node, active queries cannot complete if they require additional time to complete beyond the default five second wait period. In Drill 1.12 and later, you can use the Graceful Shutdown option, which transitions a Drillbit into a quiescent state in which the Drill process can complete in-progress queries before shutting down.  

###Stopping Drill   
To stop the Drill process on the node, issue the `drillbit.sh stop` command, as shown:  

       bin/drillbit.sh stop   

###Graceful Shutdown  

Graceful shutdown is enabled by default. You can gracefully shutdown a Drillbit from the command line or the Drill Web Console. When you initiate a graceful shutdown from the Drill Web Console, the console posts an alert stating that a graceful shutdown was triggered. You can see the progress of the shutdown as the Drillbit completes queries and transitions through the quiescent state.  

**How A Drillbit Shuts Down Gracefully**  

When a Drillbit shuts down gracefully, it transitions through a quiescent state to complete in-progress queries. 

The following list describes the various states that a Drillbit transitions through, including the quiescent state and phases within that state:  

- **Start**: The Drillbit is initializing. For example, when you issue the `drillbit.sh start` or `drillbit.sh restart` command.  
- **Online**: The Drillbit has started and registered with the cluster coordinator, such as ZooKeeper. ZooKeeper then shares the state of the Drillbit with other Drillbits in the cluster. Drillbits in the online state can all accept and process queries.  
- **Quiescent**: When a Drillbit receives a graceful shutdown request, the Drillbit transitions into the quiescent state and shares its status change with the ZooKeeper. The ZooKeeper notifies the other Drillbits in the cluster of the Drillbit’s status change. Once the other Drillbits get the status update, they do not assign work to the Drillbit. However, if the Foreman assigns work to the Drillbit as the status update occurs, the Drillbit waits to complete work before shutting down. The quiescent state has three phases: grace, draining, and offline.  
       - **Grace**: A period in which a Drillbit can accept new queries from the Foreman. You can configure (at the system level) the amount of time a Drillbit can remain in this phase using the `drill.exe.grace_period_ms` option. Set this value in milliseconds. The default value is 0. There is no maximum limit. Ideally, you should give as little time as possible or no longer than twice the default heartbeat time of the ZooKeeper. For example, if the heartbeat is 5 seconds, set the value to the equivalent of 10 seconds in milliseconds (10000).   
       - **Draining**:  When the grace period ends, the Drillbit enters the draining phase of the quiescent state. In this phase, the Drillbit cannot accept new queries, but waits for the running queries to complete. You can view the draining queries in the Drill Web Console. The Index page in the Web Console shows the queries and fragments currently running on the node.  
       - **Offline**: The phase the Drillbit enters after completing all queries.   
- **Shutdown**: The final state in which a Drillbit removes itself from the ZooKeeper registry.  

**Shutting Down a Drillbit Gracefully**  
You can gracefully shutdown a node from the command line or the Drill Web Console. 
 
From the command line, run the following command on the node you want to shut down:  

       drillbit.sh graceful_stop

From the Drill Web Console, enter the following URL in your browser’s address bar:

       http://<IP address or host name>:8047 or https://<IP address or host name>:8047

In the Drill Web Console, open the Index page, and click Shutdown next to the Drillbit you want to shut down.
  





