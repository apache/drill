---
title: "Starting Drill on Linux and Mac OS X"
parent: "Installing Drill in Embedded Mode"
---
Launch SQLLine using the sqlline command to start to Drill in embedded mode. The command directs SQLLine to connect to Drill using jdbc. The zk=local means the local node is the zookeeper node. Complete the following steps to launch SQLLine and start Drill:

1. Navigate to the Drill installation directory. For example:  

        cd apache-drill-0.9.0  

2. Issue the following command to launch SQLLine:

        bin/sqlline -u jdbc:drill:zk=local  

   The `0: jdbc:drill:zk=local>`  prompt appears.  

   At this point, you can [submit queries]({{site.baseurl}}/docs/drill-in-10-minutes#query-sample-data) to Drill.

You can use the schema option in the **sqlline** command to specify a storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query: For example, this command specifies the `dfs` storage plugin.

    bin/sqlline –u jdbc:drill:schema=dfs;zk=local
