---
title: "Connecting Drill Explorer to Data"
parent: "Using Drill Explorer"
---
The way you start Drill Explorer differs depending on your platform, but regardless of the platform, Drill must be running. On a single-node, embedded Drill cluster, start the Drill shell. On a distributed Drill cluster, start the Drillbit.

## Connecting Drill Explorer to Data on Mac OS X

1. On a node that is running Drill, go to the `/Applications` directory, and run the Drill Explorer app.  
   The Drill Explorer console appears.  
2. Click **Connect** on the console.  
   The Drill Explorer dialog appears.  
   ![sample mapr drill dsn]({{ site.baseurl }}/docs/img/explorer-connect.png)
3. If you connect through a DSN, on the ODBC DSN tab, select the name of the DSN you configured. For example, select Sample MapR Drill DSN.  
   Alternatively, if you use a DSN-less connection, on the Advanced tab, type a connection string in the text box. For example, type the following connection string:  
         DRIVER=MapR Drill ODBC Driver;AdvancedProperties={HandshakeTimeout=0;QueryTimeout=0;TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys, INFORMATION_SCHEMA;};Catalog=DRILL;Schema=;ConnectionType=ZooKeeper;ZKQuorum=192.168.39.43:5181;ZKClusterID=drillbits1
5. If you set the [AuthenticationType property]({{ site.baseurl }}/docs/odbc-configuration-reference/#configuration-options) to Basic Authentication, which enables impersonation, respond to the prompt for a user name and password.
4. Click **Connect** in the dialog.  
   In the Schemas panel, the [schema]({{ site.baseurl }}/docs/odbc-configuration-reference/#schema) for the connected data source appear. For example, the default schema is `dfs`, which points to the local file system on your machine.  
   ![explorer schemas]({{ site.baseurl }}/docs/img/explorer-schemas.png) 

## Starting Drill Explorer on Windows

1. To launch the ODBC Administrator, click **Start > All Programs > MapR Drill ODBC Driver 1.0 (32|64-bit) > (32|64-bit) ODBC Administrator**.
2. Click the **User DSN** tab or the **System DSN** tab and then select the DSN that corresponds to the Drill data source that you want to explore.
3. Click **Configure**.  
   The _MapR Drill ODBC Driver DSN Setup_ dialog appears.
4. Click **Drill Explorer**.






