---
title: "Using Apache Drill with Tibco Spotfire Desktop"
parent: "ODBC/JDBC Interfaces"
---
Tibco Spotfire Desktop is a powerful analytic tool that enables SQL statements when connecting to data sources. Spotfire Desktop can utilize the powerful query capabilities of Apache Drill to query complex data structures. Use the MapR Drill ODBC Driver to configure Tibco Spotfire Desktop with Apache Drill.

To use Spotfire Desktop with Apache Drill, complete the following steps:

1.  Install the Drill ODBC Driver from MapR.
2.	Configure the Spotfire Desktop data connection for Drill.

----------


### Step 1: Install and Configure the MapR Drill ODBC Driver 

Drill uses standard ODBC connectivity to provide easy data exploration capabilities on complex, schema-less data sets. Verify that the ODBC driver version that you download correlates with the Apache Drill version that you use. Ideally, you should upgrade to the latest version of Apache Drill and the MapR Drill ODBC Driver. 

Complete the following steps to install and configure the driver:

1.    Download the 64-bit MapR Drill ODBC Driver for Windows from the following location:<br> [http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/)     
**Note:** Spotfire Desktop 6.5.1 utilizes the 64-bit ODBC driver.
2.    Complete steps 2-8 under on the following page to install the driver:<br> 
[http://drill.apache.org/docs/step-1-install-the-mapr-drill-odbc-driver-on-windows/](http://drill.apache.org/docs/step-1-install-the-mapr-drill-odbc-driver-on-windows/)
3.    Complete the steps on the following page to configure the driver:<br>
[http://drill.apache.org/docs/step-2-configure-odbc-connections-to-drill-data-sources/](http://drill.apache.org/docs/step-2-configure-odbc-connections-to-drill-data-sources/)

----------


### Step 2: Configure the Spotfire Desktop Data Connection for Drill 
Complete the following steps to configure a Drill data connection: 

1. Select the **Add Data Connection** option or click the Add Data Connection button in the menu bar, as shown in the image below:![]({{site.baseurl}}/docs/img/spotfire_1.png)
2. When the dialog window appears, click the **Add** button, and select **Other/Database** from the dropdown list.![]({{site.baseurl}}/docs/img/spotfire_2.png)
3. In the Open Database window that appears, select **Odbc Data Provider** and then click **Configure**. ![]({{site.baseurl}}/docs/img/spotfire_3.png)
4. In the Configure Data Source Connection window that appears, select the Drill DSN that you configured in the ODBC administrator, and enter the relevant credentials for Drill.<br> ![]({{site.baseurl}}/docs/img/spotfire_4.png) 
5. Click **OK** to continue. The Spotfire Desktop queries the Drill metadata for available schemas, tables, and views. You can navigate the schemas in the left-hand column. After you select a specific view or table, the relevant SQL displays in the right-hand column. 
![]{{site.baseurl}}/docs/img/spotfire_5.png)
6. Optionally, you can modify the SQL to work best with Drill. Simply change the schema.table.* notation in the SELECT statement to simply * or the relevant column names that are needed. 
Note that Drill has certain reserved keywords that you must put in back ticks [ ` ] when needed. See [Drill Reserved Keywords](http://drill.apache.org/docs/reserved-keywords/).
7. Once the SQL is complete, provide a name for the Data Source and click **OK**. Spotfire Desktop queries Drill and retrieves the data for analysis. You can use the functionality of Spotfire Desktop to work with the data.
![]({{site.baseurl}}/docs/img/spotfire_6.png)

**NOTE:** You can use the SQL statement column to query data and complex structures that do not display in the left-hand schema column. A good example is JSON files in the file system.

**SQL Example:**<br>
SELECT t.trans_id, t.`date`, t.user_info.cust_id as cust_id, t.user_info.device as device FROM dfs.clicks.`/clicks/clicks.campaign.json` t

----------
