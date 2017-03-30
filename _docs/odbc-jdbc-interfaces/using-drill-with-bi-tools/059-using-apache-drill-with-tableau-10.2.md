---
title: "Using Apache Drill with Tableau 10.2"
date: 2017-03-30 23:49:58 UTC
parent: "Using Drill with BI Tools"
---  

**Author:** Andries Engelbrecht, Partner Systems Engineer, MapR Technologies  

----------  

The powerful combination of Drill and Tableau enables you to easily and directly work with various data formats and sources, elevating data self-service and discovery to a new level. 

Drill 1.10 fully supports Tableau Level of Detail (LoD) calculations and Tableau Sets for an enhanced user experience. Drill can also be used as a data source for Tableau Desktop on Mac, in addition to Tableau Desktop and Tableau Server on Windows.  

---------- 
  
  
This document describes how to connect Tableau 10.2 to Apache Drill and instantly explore multiple data formats from various data sources.  

----------  

###Prerequisites  

Your system must meet the following prerequisites before you can complete the steps required to connect Tableau 10.2 to Apache Drill:  

- Tableau 10.2 or later  
- Apache Drill 1.10 or later  
- MapR Drill ODBC Driver v1.3.0 or later  

----------  

###Required Steps  
 
Complete the following steps to use Apache Drill with Tableau 10.2:  
1.	[Install and Configure the MapR Drill ODBC Driver.]({{site.baseurl}}/docs/using-apache-drill-with-tableau-10.2)  
2.	[Connect Tableau to Drill (using the Apache Drill Data Connector).]({{site.baseurl}}/docs/using-apache-drill-with-tableau-10.2)  
3.	[Query and Analyze the Data (various data formats with Tableau and Drill).]({{site.baseurl}}/docs/using-apache-drill-with-tableau-10.2)  

---------- 

 

### Step 1: Install and Configure the MapR Drill ODBC Driver  
  
Drill uses standard ODBC connectivity to provide you with easy data exploration capabilities on complex, schema-less data sets. 

To install and configure the ODBC driver, complete the following steps:  

1. Download the latest 64-bit MapR Drill ODBC Driver for Mac or Windows at: [http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/)  
2. Refer to the instructions appropriate for your system to install the ODBC driver: 
       * [Windows](http://drill.apache.org/docs/installing-the-driver-on-windows/)
       * [Mac](http://drill.apache.org/docs/installing-the-driver-on-mac-os-x/)

**Important:** Verify that the Tableau client system can resolve the hostnames for the Drill and Zookeeper nodes correctly. See the *System Requirements* section of the ODBC [Mac](http://drill.apache.org/docs/installing-the-driver-on-mac-os-x/) or [Windows](http://drill.apache.org/docs/installing-the-driver-on-windows/) installation page for instructions.  

----------  


### Step 2: Connect Tableau to Drill  

To connect Tableau to Drill, complete the following steps:
 
**Note:** The [Tableau documentation](http://onlinehelp.tableau.com/current/pro/desktop/en-us/help.htm#examples_apachedrill.html) provides additional details, if needed.  

1.	In a Tableau Workbook, click on **Data > New Data Source**.
2.	Select **Apache Drill**. ![tab workbook]({{ site.baseurl }}/docs/img/T10.2_IMG_1.png)  
3.	Choose whether to connect directly or using ZooKeeper. For production environments, connecting to ZooKeeper is recommended for resiliency and distribution of connection management on the Drill cluster.  
4.	Enter the ZooKeeper Quorum/Drill Cluster ID or Drill Direct Server and Port information. 
5.	Enter the authentication information.
6.	Click **Sign In**. Tableau connects to Drill and allows you to select various Tables and Views. ![drill connect]({{ site.baseurl }}/docs/img/T10.2_IMG_2.png)  
7.	Click on the **Schema** drop-down list to display all available Drill schemas. When you select a schema, Tableau displays available tables or views.  You can select the tables and views to build a Tableau Visualization. Additionally, you can use custom SQL by clicking on the **New Custom SQL** option. ![dfs views]({{ site.baseurl }}/docs/img/T10.2_IMG_3.png)  

**Note:** Tableau can natively work with Hive tables and Drill views. You can use custom SQL or create a view in Drill to represent the complex data in Drill data sources, such as data in files or HBase/MapR-DB tables, to Tableau. For more information, see [Tableau Examples](http://drill.apache.org/docs/tableau-examples/).  

----------  


###Step 3: Query and Analyze the Data  

Tableau can now use Drill to query various data sources and visualize the information, as shown in the following example.  

####Example  
  
A retailer has order data in CSV files on the distributed file system, product data in HBase, and customer data in Hive. The retailer wants to see the average order total by customer for each state (Tableau LoD), as well as the total number of orders and average revenue by customer for the top 5 states by revenue (Tableau Set).

To find this information, the retail business analyst completes the following steps:  

1. Creates a LoD calculation for ordertotal by customer id. ![lod calc]({{ site.baseurl }}/docs/img/T10.2_IMG_4.png)  
2. Displays the states and the average revenue by customer for each state. ![cust rev state]({{ site.baseurl }}/docs/img/T10.2_IMG_5.png)  
3. Creates a graph with the total revenue by state, ordered by highest revenue. 
4. Drags and selects the top 5 states, right-clicks, and selects Create Set.  
5. Enters a name for the set. ![set name]({{ site.baseurl }}/docs/img/T10.2_IMG_6.png)
6. Creates a table showing the total number of orders and average revenue by customer for the top 5 states by revenue and also shows the number for the states not in the top 5. ![avg rev]({{ site.baseurl }}/docs/img/T10.2_IMG_7.png)  

You have completed the tutorial for configuring Tableau 10.2 to work with Apache Drill. For additional support, see  
[https://www.tableau.com/support/drivers](https://www.tableau.com/support/drivers).







 

 
 

