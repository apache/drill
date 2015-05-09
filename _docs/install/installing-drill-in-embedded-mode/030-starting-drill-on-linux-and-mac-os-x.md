---
title: "Starting Drill on Linux and Mac OS X"
parent: "Installing Drill in Embedded Mode"
---
Launch SQLLine using the sqlline command to start to Drill in embedded mode. The command directs SQLLine to connect to Drill using jdbc. The zk=local means the local node is the ZooKeeper node. Complete the following steps to launch SQLLine and start Drill:

1. Navigate to the Drill installation directory. For example:  

        cd apache-drill-0.9.0  

2. Issue the following command to launch SQLLine:

        bin/sqlline -u jdbc:drill:zk=local  

   The `0: jdbc:drill:zk=local>`  prompt appears.  

   At this point, you can [submit queries]({{site.baseurl}}/docs/drill-in-10-minutes#query-sample-data) to Drill.

## Example of Starting Drill

The simplest example of how to start SQLLine is to identify the protocol, JDBC, and ZooKeeper node or nodes in the **sqlline** command. This example starts SQLLine on a node in an embedded, single-node cluster:

    sqlline -u jdbc:drill:zk=local

This example also starts SQLLine using the `dfs` storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query:


    bin/sqlline –u jdbc:drill:schema=dfs;zk=centos26
    
You can use the schema option in the **sqlline** command to specify a storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query: For example, this command specifies the `dfs` storage plugin.

    bin/sqlline –u jdbc:drill:schema=dfs;zk=local

## Exiting SQLLine

To exit SQLLine, issue the following command:

    !quit

## Stopping Drill

In some cases, such as stopping while a query is in progress, the `!quit` command does not stop Drill running in embedded mode. To stop the Drill process on Mac OS X and Linux, use the kill command. For example, on Mac OS X and Linux, follow these steps:

  1. Issue a CTRL Z to stop the query, then start Drill again. If the startup message indicates success, skip the rest of the steps. If not, proceed to step 2.
  2. Search for the Drill process IDs.
  
        $ ps auwx | grep drill
  3. Kill each process using the process numbers in the grep output. For example:

        $ sudo kill -9 2674 