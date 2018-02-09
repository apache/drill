---
title: "Using Tibco Spotfire Desktop with Drill"
date: 2018-02-09 00:16:01 UTC
parent: "Using Drill with BI Tools"
---
Tibco Spotfire Desktop is a powerful analytic tool that enables SQL statements when connecting to data sources. Spotfire Desktop can utilize the powerful query capabilities of Apache Drill to query complex data structures. Use the MapR Drill ODBC Driver to configure Tibco Spotfire Desktop with Apache Drill.

To use Spotfire Desktop with Apache Drill, complete the following steps:

1.  Install the Drill ODBC Driver from MapR.
2.  Configure the Spotfire Desktop data connection for Drill.


## Step 1: Install and Configure the MapR Drill ODBC Driver 

Drill uses standard ODBC connectivity to provide easy data exploration capabilities on complex, schema-less data sets. Verify that the ODBC driver version that you download correlates with the Apache Drill version that you use. Ideally, you should upgrade to the latest version of Apache Drill and the MapR Drill ODBC Driver. 

Complete the following steps to install and configure the driver:

1. Download the 64-bit MapR Drill ODBC Driver for Windows from the following location:<br> [http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/)     
   {% include startnote.html %}Spotfire Desktop 6.5.1 utilizes the 64-bit ODBC driver.{% include endnote.html %}
2. [Install the driver]({{site.baseurl}}/docs/installing-the-driver-on-windows). 
3. [Configure ODBC]({{site.baseurl}}/docs/configuring-odbc-on-windows).

----------


## Step 2: Configure the Spotfire Desktop Data Connection for Drill 
Complete the following steps to configure a Drill data connection: 

1. Select the **Add Data Connection** option or click the Add Data Connection button in the menu bar, as shown in the image below:![](http://i.imgur.com/p3LNNBs.png)
2. When the dialog window appears, click the **Add** button, and select **Other/Database** from the dropdown list.![](http://i.imgur.com/u1g9kaT.png)
3. In the Open Database window that appears, select **Odbc Data Provider** and then click **Configure**. ![](http://i.imgur.com/8Gu0GAZ.png)
4. In the Configure Data Source Connection window that appears, select the Drill DSN that you configured in the ODBC administrator, and enter the relevant credentials for Drill.<br> ![](http://i.imgur.com/Yd6BKls.png) 
5. Click **OK** to continue. The Spotfire Desktop queries the Drill metadata for available schemas, tables, and views. You can navigate the schemas in the left-hand column. After you select a specific view or table, the relevant SQL displays in the right-hand column. 
![](http://i.imgur.com/wNBDs5q.png)
6. Optionally, you can modify the SQL to work best with Drill. Simply change the schema.table.* notation in the SELECT statement to simply * or the relevant column names that are needed. 
Note that Drill has certain reserved keywords that you must put in back ticks [ ` ] when needed. See [Drill Reserved Keywords](http://drill.apache.org/docs/reserved-keywords/).
7. Once the SQL is complete, provide a name for the Data Source and click **OK**. Spotfire Desktop queries Drill and retrieves the data for analysis. You can use the functionality of Spotfire Desktop to work with the data.
![](http://i.imgur.com/j0MWorh.png)

**NOTE:** You can use the SQL statement column to query data and complex structures that do not display in the left-hand schema column. A good example is JSON files in the file system.

**SQL Example:**<br>
SELECT t.trans_id, t.`date`, t.user_info.cust_id as cust_id, t.user_info.device as device FROM dfs.clicks.`/clicks/clicks.campaign.json` t

----------
