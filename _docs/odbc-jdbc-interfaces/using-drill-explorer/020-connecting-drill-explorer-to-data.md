---
title: "Connecting Drill Explorer to Data"
date: 2017-06-06 23:26:14 UTC
parent: "Using Drill Explorer"
---
The way you start Drill Explorer differs depending on your platform, but regardless of the platform, Drill must be running. 

## Connecting Drill Explorer to Data on Linux

You need an X-11 XDisplay to use Drill Explorer on Linux. Run the DrillExplorer executable in `/opt/mapr/drillodbc/DrillExplorer`, and then follow instructions from step 2 in the next section, *Connecting Drill Explorer to Data on Mac OS X*.

## Connecting Drill Explorer to Data on Mac OS X

1. On a node that is running Drill, run Drill Explorer.  The Drill Explorer app is located in the `/Applications` directory. The Drill Explorer console appears. 
 
2. Click **Connect** on the console. The Drill Explorer dialog appears.  
 
	![](http://i.imgur.com/vvr1vs4.png)

3. If you connect through a DSN, select the name of the DSN you configured on the **ODBC DSN** tab. For example, select **MapR Drill**. Alternatively, if you use a DSN-less connection, type a connection string in the text box on the **Advanced** tab.. For example, type the following connection string:  
 
         DRIVER=MapR Drill ODBC Driver;AdvancedProperties={HandshakeTimeout=0;QueryTimeout=0;TimestampTZDisplayTimezone=utc;ExcludedSchemas=sys, INFORMATION_SCHEMA;};Catalog=DRILL;Schema=;ConnectionType=ZooKeeper;ZKQuorum=192.168.39.43:5181;ZKClusterID=drillbits1

5. If you set the Authentication Type property to **Plain** (or Basic Authentication), which enables impersonation, respond to the prompt for a user name and password.

4. Click **Connect** in the dialog. In the **Schemas** panel, the schema for the connected data source appear.   
 
	![](http://i.imgur.com/PZaVPRf.png)

## Starting Drill Explorer on Windows

In Windows 10, you can start Drill Explorer in two ways::

- Start menu 
- ODBC Data Source Administrator

### Start Menu
To start Drill Explorer from the Start menu:

1. Click **Start**, and locate **MaprR Drill Driver 1.3 <*version*>**. 

2. Click **Drill Explorer**.
   
	![](http://i.imgur.com/qswpcKS.png)
   
  The **ODBC Connection** dialog with the **DSN** tab displayed appears in Drill Explorer.

![](http://i.imgur.com/W1CQwH0.png)

3\. Select the DSN that you want to explore. For example, select **MapR Drill** and click **Connect**.


### ODBC Data Source Administrator   
To start Drill Explorer from the ODBC Data Source Administrator:

1. Enter **odbc** and select a version of the utility from the Windows Settings field. The **ODBC Data Source Administrator <*version*>** dialog appears. 
   
	![](http://i.imgur.com/W9ZO5PN.png)
   
3. Click the  **System DSN** tab.
 
5. Select **MapR Drill** and click **Configure**. The **MapR Drill ODBC Driver DSN Setup** dialog appears showing multiple configuration properties.  
   
	![](http://i.imgur.com/FlRRuSm.png)

4. Click **Drill Explorer** at the bottom of the dialog. Drill Explorer appears.








