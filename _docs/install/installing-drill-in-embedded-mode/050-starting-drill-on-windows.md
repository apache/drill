---
title: "Starting Drill on Windows"
parent: "Installing Drill in Embedded Mode"
---
Launch SQLLine using the **sqlline command** to start to Drill in embedded mode. The command directs SQLLine to connect to Drill. The `zk=local` means the local node is the zookeeper node. Complete the following steps to launch SQLLine and start Drill:

1. Open the apache-drill-0.9.0 folder.  
2. Open the bin folder, and double-click the `sqlline.bat` file:
   ![drill bin dir]({{ site.baseurl }}/docs/img/drill-bin.png)
   The Windows command prompt opens.  
3. At the sqlline> prompt, type `!connect jdbc:drill:zk=local` and then press Enter:
   ![sqlline]({{ site.baseurl }}/docs/img/sqlline1.png)
4. Enter the username, `admin`, and password, also `admin` when prompted.
   The `0: jdbc:drill:zk=local>` prompt appears.
At this point, you can [submit queries]({{ site.baseurl }}/docs/drill-in-10-minutes#query-sample-data) to Drill.

You can use the schema option in the **sqlline** command to specify a storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query: For example, this command specifies the `dfs` storage plugin.

    bin/sqlline –u jdbc:drill:schema=dfs;zk=local