---
title: "Using Apache Drill with Tableau 9 Server"
date: 2018-02-09 00:16:02 UTC
parent: "Using Drill with BI Tools"
---

This document describes how to connect Tableau 9 Server to Apache Drill and explore multiple data formats instantly on Hadoop, as well as share all the Tableau visualizations in a collaborative environment. Use the combined power of these tools to get direct access to semi-structured data, without having to rely on IT teams for schema creation and data manipulation. 

To use Apache Drill with Tableau 9 Server, complete the following steps: 

1.	Install the Drill ODBC driver from MapR on the Tableau Server system and configure ODBC data sources.
2.	Install the Tableau Data-connection Customization (TDC) file.
3.	Publish Tableau visualizations and data sources from Tableau Desktop to Tableau Server for collaboration.


## Step 1: Install and Configure the MapR Drill ODBC Driver 

Drill uses standard ODBC connectivity to provide easy data-exploration capabilities on complex, schema-less data sets. The latest release of Apache Drill. For Tableau 9.0 Server, Drill Version 0.9 or higher is recommended.

Complete the following steps to install and configure the driver:

1. Download the 64-bit MapR Drill ODBC Driver for Windows from the following location:<br> [http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/)     
**Note:** Tableau 9.0 Server works with the 64-bit ODBC driver.
2. [Install the 64-bit ODBC driver on Windows]({{site.baseurl}}/docs/installing-the-driver-on-windows/).
3. [Configure the driver]({{site.baseurl}}/docs/configuring-odbc-on-windows/).
4. If Drill authentication is enabled, select **Basic Authentication** as the authentication type. Enter a valid user and password. ![drill query flow]({{ site.baseurl }}/docs/img/tableau-odbc-setup.png)

**Note:** If you select **ZooKeeper Quorum** as the ODBC connection type, the client system must be able to resolve the hostnames of the ZooKeeper nodes. The simplest way is to add the hostnames and IP addresses for the ZooKeeper nodes to the `%WINDIR%\system32\drivers\etc\hosts` file. ![drill query flow]({{ site.baseurl }}/docs/img/tableau-odbc-setup-2.png)

Also make sure to test the ODBC connection to Drill before using it with Tableau.



## Step 2: Install the Tableau Data-connection Customization (TDC) File

The MapR Drill ODBC Driver includes a file named `MapRDrillODBC.TDC`. The TDC file includes customizations that improve ODBC configuration and performance when using Tableau.

For Tableau Server, you need to manually copy this file to the Server Datasources folder:
1.	Locate the `MapRDrillODBC.tdc` file in the `~\Program Files\MapR Drill ODBC Driver\Resources` folder.
2.	Copy the file to the `~\ProgramData\Tableau\Tableau Server\data\tabsvc\vizqlserver\Datasources` folder.
3.	Restart Tableau Server.

For more information about Tableau TDC configuration, see [Customizing and Tuning ODBC Connections](http://kb.tableau.com/articles/knowledgebase/customizing-odbc-connections)



## Step 3: Publish Tableau Visualizations and Data Sources

For collaboration purposes, you can now use Tableau Desktop to publish data sources and visualizations on Tableau Server.

###Publishing Visualizations

To publish a visualization from Tableau Desktop to Tableau Server:

1. Configure Tableau Desktop by using the ODBC driver; see []()

2. For best results, verify that the ODBC configuration and DSNs (data source names) are the same for both Tableau Desktop and Tableau Server.

3. Create visualizations in Tableau Desktop using Drill as the data source.

4. Connect to Tableau Server from Tableau Desktop. Select **Server > Sign In**. ![drill query flow]({{ site.baseurl }}/docs/img/tableau-server-signin1.png)

5. Sign into Tableau Server using the server hostname or IP address, username, and password. ![drill query flow]({{ site.baseurl }}/docs/img/tableau-server-signin2.png)

6. You can now publish a workbook to Tableau Server. Select **Server > Publish Workbook**. ![drill query flow]({{ site.baseurl }}/docs/img/tableau-server-publish1.png)

7. Select the project from the drop-down list. Enter a name for the visualization to be published and provide a description and tags as needed. Assign permissions and views to be shared. Then click **Authentication**. ![drill query flow]({{ site.baseurl }}/docs/img/tableau-server-publish2.png)

8. In the Authentication window, select **Embedded Password**, then click **OK**. Then click **Publish** in the Publish Workbook window to publish the visualization to Tableau Server. ![drill query flow]({{ site.baseurl }}/docs/img/tableau-server-authentication.png)

###Publishing Data Sources

If all you want to do is publish data sources to Tableau Server, follow these steps:
1.	Open data source(s) in Tableau Desktop.
2.	In the Workbook, select **Data > Data Source Name > Publish to Server**. ![drill query flow]({{ site.baseurl }}/docs/img/tableau-server-publish-datasource.png)

3.	If you are not already signed in, sign into Tableau Server.
4.	Select the project from the drop-down list and enter a name for the data source (or keep the same name that is used in the Desktop workbook). ![drill query flow]({{ site.baseurl }}/docs/img/tableau-server-publish-datasource2.png)

5.	In the **Authentication** drop-down list, select **Embedded Password**. Select permissions as needed, then click **Publish**. The data source will now be published on the Tableau Server and is available for building visualizations. ![drill query flow]({{ site.baseurl }}/docs/img/tableau-server-publish-datasource3.png)


In this quick tutorial, you saw how you can configure Tableau Server 9.0 to work with Tableau Desktop and Apache Drill. 

