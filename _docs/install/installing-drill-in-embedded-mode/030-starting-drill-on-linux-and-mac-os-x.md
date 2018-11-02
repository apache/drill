---
title: "Starting Drill on Linux and Mac OS X"
date: 2018-11-02
parent: "Installing Drill in Embedded Mode"
---
To start the Drill shell in embedded mode, use the `drill-embedded` command. Internally, the command uses a jdbc connection string and identifies the local node as the ZooKeeper node. Complete the following steps to start the Drill shell:

1. Navigate to the Drill installation directory.

2. Issue the following command to start the Drill shell:

    `bin/drill-embedded`  

   The `0: jdbc:drill:zk=local>`  prompt appears. 

   At this point, you can [run queries]({{site.baseurl}}/docs/query-data).

To start Drill, you can also use the **sqlline** command and a custom connection string, as described in detail in ["Using an Ad-Hoc Connection to Drill"]({{site.baseurl}}/docs/starting-drill-in-distributed-mode/#using-an-ad-hoc-connection-to-drill). For example, you can specify the default storage plugin configuration when you start the shell. Doing so eliminates the need to specify the storage plugin configuration in the query. For example, this command specifies the `dfs` storage plugin:

`bin/sqlline –u jdbc:drill:zk=local;schema=dfs`

If you start Drill on one network, and then want to use Drill on another network, such as your home network, restart Drill.

## About the Drill Prompt

In embedded mode, the Drill prompt appears as follows:

`0: jdbc:drill:zk=local>`

* 0 is the number of connections to Drill, which can be only one in embedded node. 
* jdbc is the connection type.
* zk=local zk=local means the local node substitutes for the ZooKeeper node.

## Exiting the Drill Shell

To exit the Drill shell and stop the Drill process on Mac OS X and Linux, issue the following command:

`!quit`

