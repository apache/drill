---
title: "Using Qlik Sense with Drill"
date: 2018-02-09 00:16:01 UTC
parent: "Using Drill with BI Tools"
---
You can use the Qlik Sense BI tool with Apache Drill, the SQL query engine for Big Data exploration, to access and analyze structured and semi-structured data in multiple data stores.  
 
This document provides you with the procedures required to connect Qlik Sense Desktop and Qlik Sense Server to Apache Drill via ODBC.

To use Qlik Sense with Apache Drill, complete the following steps:

1.     Install and configure the Drill ODBC driver.
2.	Configure a connection in Qlik Sense.
3.	Authenticate.
4.	Select tables and load the data model.
5.	Analyze data with Qlik Sense and Drill.  

##Prerequisites  
 
*  Apache Drill installed. See [Install Drill]({{site.baseurl}}/docs/install-drill/).  
*  Qlik Sense installed. See [Qlik Sense](http://www.qlik.com/us/explore/products/sense).



## Step 1: Install and Configure the Drill ODBC Driver 

Drill uses standard ODBC connectivity to provide easy data exploration capabilities on complex, schema-less data sets. Verify that the ODBC driver version that you download correlates with the Apache Drill version that you use. Ideally, you should upgrade to the latest version of Apache Drill and the MapR Drill ODBC Driver. 

Complete the following steps to install and configure the driver:

1. Download the 64-bit MapR Drill ODBC Driver for Windows from the following location:<br> [http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/](http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/)     
2. [Install the driver]({{site.baseurl}}/docs/installing-the-driver-on-windows). 
3. [Configure ODBC]({{site.baseurl}}/docs/configuring-odbc-on-windows).



## Step 2: Configure a Connection in Qlik Sense  
Once you create an ODBC DSN, it shows up as another option when you create a connection from a new and/or existing Qlik Sense application. The steps for creating a connection from an application are the same in Qlik Sense Desktop and Qlik Sense Server. 
 
Complete the following steps to configure a Drill data connection: 

1. In the Data Load Editor, click **Create new connection**.
2. Select the **ODBC** option.
3. Click either the **User** or **System DSN** button, depending on whether the DSN was created as a User or System DSN.
4. Select the appropriate DSN and provide the credentials, and name the connection accordingly.  
![]({{ site.baseurl }}/docs/img/step3_img1.png)
 
## Step 3: Authenticate  
After providing the credentials and saving the connection, click **Select** in the new connection to trigger the authentication against Drill.  

![]({{ site.baseurl }}/docs/img/step4_img1.png)  

Based on the user’s credentials, security and filtration are applied accordingly. Different users may see a different number of tables and/or a different number of fields per table. For example, multiple types of users may use the same connection, but they may see a different number of tables and columns per table. For example, a manager may only see one table and a few fields in the table.    

![]({{ site.baseurl }}/docs/img/step4_img2.png)  

While an Executive may have access to more tables and more fields per table.  

![]({{ site.baseurl }}/docs/img/step4_img3.png)
 

## Step 4: Select Tables and Load the Data Model  

Explore the various tables available in Drill, and select the tables of interest. For each table selected, Qlik Sense shows a preview of the logic used for the table.  

![]({{ site.baseurl }}/docs/img/step5_img1.png)  

Notice that the metadata information that comes with each table is also accessible through the same window.  

![]({{ site.baseurl }}/docs/img/step5_img2.png)  

Click **Insert Script** to add a new table as part of the associative data model that the Qlik Sense application creates. There are two of ways in which you can make a new table part of the data model.

1. Load the data from the table into the memory of the Qlik Sense Server.  
2. Keep the data at the source and only capture the new table as part of the data model (this is called Direct Discovery). In order to learn more about Direct Discovery, see [http://www.qlik.com/us/explore/resources/whitepapers/qlikview-and-big-data](http://www.qlik.com/us/explore/resources/whitepapers/qlikview-and-big-data).  

{% include startnote.html %}After you select the tables that you want to include, verify that the top part of the script is set to the following, otherwise the load fails:{% include endnote.html %}  

       SET DirectIdentifierQuoteChar="`"   

![]({{ site.baseurl }}/docs/img/step5_img3.png) 

When the data model is complete, click **Load Data**. Verify that the load is completed successfully.  

![]({{ site.baseurl }}/docs/img/step5_img4.png)  

If a Direct Discovery is used, the syntax of the script, as well as the messages that Qlik Sense displays while loading the data model vary slightly.  

![]({{ site.baseurl }}/docs/img/step5_img5.png)  
 

## Step 5: Analyze Data with Qlik Sense and Drill  

After the data model is loaded into the application, use Qlik Sense to build a wide range of visualizations on top of the data that Drill delivers via ODBC. Qlik Sense specializes in self-service data visualization at the point of decision.  

![]({{ site.baseurl }}/docs/img/step6_img1.png)  

If you use Direct Discovery to build the application, the application becomes a hybrid application. This means that some of the fields and tables are kept in memory as part of the application definition while other tables are kept in the data model only at the metadata level while the data behind them resides at the source. In such cases, any given visualization could end up representing the combination of in memory data and data polled in real time from Drill.  

![]({{ site.baseurl }}/docs/img/step6_img2.png)
  


## Summary 
Together, Drill and Qlik Sense can provide a wide range of solutions that enable organizations to analyze all of their data and efficiently find solutions to various business problems.
 
To continue exploring Qlik Sense and download Qlik Sense Desktop, visit   
[http://www.qlik.com/us/explore/products/sense](http://www.qlik.com/us/explore/products/sense)  

For more information about Drill, visit  
[http://drill.apache.org/](http://drill.apache.org/)


  

