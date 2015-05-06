---
title: "Starting/Stopping Drill"
parent: "Manage Drill"
---
How you start Drill depends on the installation method you followed. If you installed Drill in embedded mode, invoking SQLLine automatically starts a Drillbit locally. If you installed Drill in distributed mode, and the Drillbit on a node did not start, start the Drillbit before attempting to run queries. How to start Drill is covered in detail the section, ["Install Drill"]({{site.baseurl}}/docs/install-drill/).

## Examples of Starting Drill
Issue the **sqlline** command from the Drill installation directory. The simplest example of how to start SQLLine is to identify the protocol, JDBC, and zookeeper node or nodes in the **sqlline** command. This example starts SQLLine on a node in an embedded, single-node cluster:

    sqlline -u jdbc:drill:zk=local

This example also starts SQLLine using the `dfs` storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query:


    bin/sqlline –u jdbc:drill:schema=dfs;zk=centos26

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

