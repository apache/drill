---
title: "Connecting Drill Explorer to Data"
parent: "Using Drill Explorer"
---
The way you start Drill Explorer differs depending on your platform, but regardless of the platform, Drill must be running. 

## Connecting Drill Explorer to Data on Linux

You need an X-11 XDisplay to use Drill Explorer on Linux. Run the DrillExplorer executable in `/opt/mapr/drillodbc/DrillExplorer`, and then follow instructions from step 2 in the next section, "Connecting Drill Explorer to Data on Mac OS X."

## Connecting Drill Explorer to Data on Mac OS X

1. On a node that is running Drill, run Drill Explorer.  The Drill Explorer app is located in the `/Applications` directory.  
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

You can start Drill Explorer from Apps or from the ODBC Administrator.

To start Drill Explorer from Apps:

1. Click **Start**, and locate the MaprR Drill ODBC Driver 1.0 app group in Apps.
   The MapR Drill ODBC Driver 1.0 group includes Drill Explorer.
   ![]({{ site.baseurl }}/docs/img/odbc-user-dsn.png)

2. Click **Drill Explorer**.
   The ODBC Connection dialog appears.
3. Select the DSN that you want to explore. For example, select the sample MapR ODBC Driver for Drill DSN.
   ![]({{ site.baseurl }}/docs/img/odbc-explorer-connect.png)
   Drill Explorer appears.
   ![]({{ site.baseurl }}/docs/img/odbc-explorer-win.png)

To start Drill Explorer from the ODBC Administrator:

1. In ODBC Administrator, on the **System DSN** tab in System Data Sources, a DSN. For example, select the sample MapR ODBC Driver for Drill DSN.  
   ![]({{ site.baseurl }}/docs/img/odbc-configure1.png)
   Alternatively, if you set up a user DSN, select **User DSN**, and select a DSN.
3. Click **Configure**.  
   The DSN Setup dialog appears showing the configured properties.  
   ![]({{ site.baseurl }}/docs/img/odbc-configure2.png)
4. Click **Drill Explorer** at the bottom of the dialog.
   Drill Explorer appears.






