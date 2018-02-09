---
title: "Using Information Builders’ WebFOCUS with Apache Drill"
date: 2018-02-09 00:16:03 UTC
parent: "Using Drill with BI Tools"
---

This document describes how to configure Information Builders’ WebFOCUS 8.2 with Apache Drill. You can use WebFOCUS with Drill to couple the visualization and analytic capabilities of WebFOCUS with the powerful SQL capabilities of Drill.  

Complete the following steps to configure WebFOCUS with Apache Drill:  

1. Install the Drill JDBC Driver on a Linux or Windows system configured with the WebFOCUS environment.  
2. Configure the WebFOCUS adapter and connections to Drill.  
3. (Optional) Create additional Drill connections.  

##Prerequisite  

Drill 1.2 or later



## Step 1: Install the Apache Drill JDBC driver.  

Drill provides JDBC connectivity that easily integrates with WebFOCUS. See [{{site.baseurl}}/docs/using-the-jdbc-driver/](https://drill.apache.org/docs/using-the-jdbc-driver/) for general installation steps.  

Complete the following steps to install the driver:  

1. Locate the Drill JDBC driver in the Drill installation directory on any node in the cluster with Drill installed:  
`<drill-home>/jars/jdbc-driver/drill-jdbc-all-<drill-version>.jar`  
The following example shows the location of the driver on a MapR cluster:  
`/opt/mapr/drill/drill-1.4.0/jars/jdbc-driver/drill-jdbc-all-1.4.0.jar`
2.  Copy the Drill JDBC driver to a directory on the WebFOCUS system.  
The following example shows the driver JAR file copied to a directory on a Linux server.  
`/usr/lib/drill-1.4.0/jdbc-driver/drill-jdbc-all-1.4.0.jar`



## Step 2: Configure the WebFOCUS adapter and connections to Drill.  

1. From a web browser, access the WebFOCUS Management Console. The WebFOCUS administrator provides you with the URL information: `http://hostname:port/`  
The default port is 8121.
2. Click **Adapters** in the WebFOCUS Management Console.  
![](http://i.imgur.com/owkjMKU.png)  
The Apache Drill adapter appears in the list.  
![](http://i.imgur.com/4y5EAzK.png) 
3. Right-click on the Apache Drill adapter, and select **Configure** to open the configuration form.
4. Complete the configuration form. For security, provide the User and Password credentials for the Drill cluster.  
![](http://i.imgur.com/estSqu0.png)  
 
5. After completing the configuration form click **Configure**. When the adapter is configured, the following dialog appears:  
![](http://i.imgur.com/qDbOtXa.png)
6. Click **Test** to verify the configuration. If the configuration is successful, you see a window similar to the following, otherwise you get error messages that need to be resolved:  
![](http://i.imgur.com/072YTag.png)  
Now you can use the WebFOCUS adapter and connection or create additional connections.



## (Optional) Step 3: Create additional Drill connections. 

Complete the following steps to create additional connections:  

1. Right-click on the configured Drill adapter, and select **Add connection**.  
![](http://i.imgur.com/o06bn15.png)  
2. Right-click on the connection, and select **Create or Update Synonym** to gather metadata.  
![](http://i.imgur.com/7BvXalY.png)    
3. In the Synonym Candidates dialog, click **Next**.  
![](http://i.imgur.com/lXnd0VK.png)
4. In the Create Synonym for Apache Drill dialog, select the WebFOCUS Application folder where you want to store your metadata, select the tables and views to use, and click **Next**.  
![](http://i.imgur.com/GbBOo59.png)  
You should receive a message stating that the WebFOCUS metadata (synonyms) for the Apache Drill objects were created successfully. Click **Close** to exit the message.
5. Verify that WebFOCUS can access the Apache Drill objects.  
       * Select the Application folder that contains the metadata created in the previous steps.  
       * Highlight the object that was created.  
6. Right-click on that object, and select **Sample Data**. You should see a retrieval of at least 50 records or possible error messages. If you get error messages, you must troubleshoot them.  

Drill is now configured as a data source that WebFOCUS can use to create content, such as reports, charts, and graphs, and to perform analytics against.

