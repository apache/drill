---
title: "Starting Drill on Windows"
parent: "Installing Drill in Embedded Mode"
---
Start the Drill shell using the **sqlline command**. The `zk=local` means the local node is the ZooKeeper node. Complete the following steps to launch the Drill shell:

1. Open Command Prompt.  
2. Open the apache-drill-1.0.0 folder. For example:  
   ``cd apache-drill-1.0.0``
3. Go to the bin directory. For example:  
   ``cd bin``
4. Type the following command on the command line:
   ``sqlline.bat -u "jdbc:drill:zk=local"``
   ![drill install dir]({{ site.baseurl }}/docs/img/sqlline1.png)

At this point, you can [submit queries]({{ site.baseurl }}/docs/drill-in-10-minutes#query-sample-data) to Drill.

You can use the schema option in the **sqlline** command to specify a storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query. For example, this command specifies the `dfs` storage plugin:

    C:\bin\sqlline sqlline.bat –u "jdbc:drill:schema=dfs;zk=local"

## Exiting the Drill Shell

To exit the Drill shell, issue the following command:

    !quit

## Stopping Drill

In some cases, such as stopping while a query is in progress, the `!quit` command does not stop Drill running in embedded mode. To stop the Drill process use the [**TaskKill**](https://www.microsoft.com/resources/documentation/windows/xp/all/proddocs/en-us/taskkill.mspx?mfr=true) command.

