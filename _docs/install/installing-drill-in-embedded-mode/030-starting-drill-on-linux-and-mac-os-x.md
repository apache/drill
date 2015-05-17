---
title: "Starting Drill on Linux and Mac OS X"
parent: "Installing Drill in Embedded Mode"
---
Start the Drill shell using the `drill-embedded` command. The command uses a jdbc connection string and identifies the local node as the ZooKeeper node. Complete the following steps to start the Drill shell:

1. Navigate to the Drill installation directory. For example:  

        cd apache-drill-1.0.0  

2. Issue the following command to launch SQLLine:

        bin/drill-embedded  

   The `0: jdbc:drill:zk=local>`  prompt appears.  

   At this point, you can [run queries]({{site.baseurl}}/docs/drill-in-10-minutes#query-sample-data).

You can also use the **sqlline** command to start Drill using a custom connection string, as described in ["Using an Ad-Hoc Connection to Drill"](docs/starting-drill-in-distributed-mode/#using-an-ad-hoc-connection-to-drill). For example, you can specify the storage plugin when you start the shell. Doing so eliminates the need to specify the storage plugin in the query: For example, this command specifies the `dfs` storage plugin.

    bin/sqlline –u jdbc:drill:schema=dfs;zk=local

## Exiting the Drill Shell

To exit the Drill shell, issue the following command:

    !quit

## Stopping Drill

In some cases, such as stopping while a query is in progress, the `!quit` command does not stop Drill running in embedded mode. To stop the Drill process on Mac OS X and Linux, use the kill command. For example, on Mac OS X and Linux, follow these steps:

  1. Issue a CTRL Z to stop the query, then start Drill again. If the startup message indicates success, skip the rest of the steps. If not, proceed to step 2.
  2. Search for the Drill process IDs.
  
        $ ps auwx | grep drill
  3. Kill each process using the process numbers in the grep output. For example:

        $ sudo kill -9 2674 