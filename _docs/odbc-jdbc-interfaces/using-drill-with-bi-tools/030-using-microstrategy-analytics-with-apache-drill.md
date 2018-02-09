---
title: "Using MicroStrategy Analytics with Apache Drill"
date: 2018-02-09 00:16:00 UTC
parent: "Using Drill with BI Tools"
---
Apache Drill is certified with the MicroStrategy Analytics Enterprise Platform™. You can connect MicroStrategy Analytics Enterprise to Apache Drill and explore multiple data formats instantly on Hadoop. Use the combined power of these tools to get direct access to semi-structured data without having to rely on IT teams for schema creation.

Complete the following steps to use Apache Drill with MicroStrategy Analytics Enterprise:
 
1.  Install the Drill ODBC driver from MapR.
2.	Configure the MicroStrategy Drill Object.
3.	Create the MicroStrategy database connection for Drill.
4.	Query and analyze the data.

----------


## Step 1: Install and Configure the MapR Drill ODBC Driver 

Drill uses standard ODBC connectivity to provide easy data exploration capabilities on complex, schema-less data sets. Verify that the ODBC driver version that you download correlates with the Apache Drill version that you use. Ideally, you should upgrade to the latest version of Apache Drill and the MapR Drill ODBC Driver. 

Complete the following steps to install and configure the driver:

1.	Download the driver from the following location: 

    http://package.mapr.com/tools/MapR-ODBC/MapR_Drill/

    {% include startnote.html %}Use the 32-bit Windows driver for MicroStrategy 9.4.1.{% include endnote.html %}

2.	Complete steps 2-8 under *Installing the Driver* on the following page: 

    https://drill.apache.org/docs/installing-the-driver-on-windows/
3.	Complete the steps on the following page to configure the driver:

    https://drill.apache.org/docs/configuring-odbc-on-windows/ 

    {% include startnote.html %}Verify that you are using the 32-bit driver since both drivers can coexist on the same machine.{% include endnote.html %} 

	a.	Verify the version number of the driver.

    	 
	b.	Click Test to verify that the ODBC configuration works before using it with MicroStrategy.

    ![]({{ site.baseurl }}/docs/img/image_2.png)

----------


## Step 2: Install the Drill Object on MicroStrategy Analytics Enterprise 
The steps listed in this section were created based on the MicroStrategy Technote for installing DBMS objects which you can reference at: 

http://community.microstrategy.com/t5/Database/TN43537-How-to-install-DBMS-objects-provided-by-MicroStrategy/ta-p/193352


Complete the following steps to install the Drill Object on MicroStrategy Analytics Enterprise:

1. Obtain the Drill Object from MicroStrategy Technical Support. The Drill Object is contained in a file named `MapR_Drill.PDS`. When you get this file, store it locally in your Windows file system.
2. Open MicroStrategy Developer. 
3. Expand Administration, and open Configuration Manager.
4. Select **Database Instances**.
   ![]({{ site.baseurl }}/docs/img/image_3.png)
5. Right-click in the area where the current database instances display. 
   ![]({{ site.baseurl }}/docs/img/image_4.png)
6. Select **New – Database Instance**. 
7. Once the Database Instances window opens, select **Upgrade**.
   ![]({{ site.baseurl }}/docs/img/image_5.png)
8. Enter the path and file name for the Drill Object file in the DB types script file field. Alternatively, you can use the browse button next to the field to search for the file. 
   ![]({{ site.baseurl }}/docs/img/image_6.png)
9.  Click **Load**. 
10.	Once loaded, select the MapR Drill database type in the left column.
11.	Click **>** to load MapR Drill into **Existing database types**. 
12.	Click **OK** to save the database type.
13.	Restart MicroStrategy Intelligence Server if it is used for the project source.
   ![]({{ site.baseurl }}/docs/img/image_7.png)

MicroStrategy Analytics Enterprise can now access Apache Drill.


----------

## Step 3: Create the MicroStrategy database connection for Apache Drill
Complete the following steps to use the Database Instance Wizard to create the MicroStrategy database connection for Apache Drill:

1. In MicroStrategy  Developer, select **Administration > Database Instance Wizard**.
   ![]({{ site.baseurl }}/docs/img/image_8.png)
2. Enter a name for the database, and select **MapR Drill** as the Database type from the drop-down menu.
   ![]({{ site.baseurl }}/docs/img/image_9.png)
3. Click **Next**. 
4. Select the ODBC DSN that you configured with the ODBC Administrator.
   ![]({{ site.baseurl }}/docs/img/image_10.png)
5. Provide the login information for the connection and then click **Finish**.

You can now use MicroStrategy Analytics Enterprise to access Drill as a database instance. 

----------


## Step 4: Query and Analyze the Data
This step includes an example scenario that shows you how to use MicroStrategy, with Drill as the database instance, to analyze Twitter data stored as complex JSON documents. 

###Scenario
The Drill distributed file system plugin is configured to read Twitter data in a directory structure. A view is created in Drill to capture the most relevant maps and nested maps and arrays for the Twitter JSON documents. Refer to [Query Data](/docs/query-data-introduction/) for more information about how to configure and use Drill to work with complex data:

###Part 1: Create a Project
Complete the following steps to create a project:

1. In MicroStrategy Developer, use the Project Creation Assistant to create a new project.
   ![]({{ site.baseurl }}/docs/img/image_11.png)
2.  Once the Assistant starts, click **Create Project**, and enter a name for the new project. 
3.	Click **OK**. 
4.	Click **Select tables from the Warehouse Catalog**. 
5.	Select the Drill database instance connection from the drop down list, and click **OK**.	MicroStrategy queries Drill and displays all of the available tables and views.
   ![]({{ site.baseurl }}/docs/img/image_12.png)
6.	Select the two views created for the Twitter Data.
7.	Use **>** to move the views to **Tables being used in the project**. 
8.	Click **Save and Close**.
9.	Click **OK**. The new project is created in MicroStrategy Developer. 

###Part 2: Create a Freeform Report to Analyze Data
Complete the following steps to create a Freeform Report and analyze data:

1.	In Developer, open the Project and then open Public Objects.
2.	Click **Reports**.
3.	Right-click in the pane on the right, and select **New > Report**.
   ![]({{ site.baseurl }}/docs/img/image_13.png)
4.	Click the **Freeform Soures** tab, and select the Drill data source.
   ![]({{ site.baseurl }}/docs/img/image_14.png)
5.	Verify that **Create Freeform SQL Report** is selected, and click **OK**. This allows you to enter a quick query to gather data. The Freeform SQL Editor window appears.
   ![]({{ site.baseurl }}/docs/img/image_15.png)
6.	Enter a SQL query in the field provided. Attributes specified display. 
In this scenario, a simple query that selects and groups the tweet source and counts the number of times the same source appeared in a day is entered. The tweet source was added as a text metric and the count as a number. 
7.	Click **Data/Run Report** to run the query. A bar chart displays the output.
   ![]({{ site.baseurl }}/docs/img/image_16.png)

You can see that there are three major sources for the captured tweets. You can change the view to tabular format and apply a filter to see that iPhone, Android, and Web Client are the three major sources of tweets for this specific data set.
![]({{ site.baseurl }}/docs/img/image_17.png)

In this scenario, you learned how to configure MicroStrategy Analytics Enterprise to work with Apache Drill. 

----------

###Certification Links

* MicroStrategy certifies its analytics platform with Apache Drill: http://ir.microstrategy.com/releasedetail.cfm?releaseid=902795

* http://community.microstrategy.com/t5/Database/TN225724-Post-Certification-of-MapR-Drill-0-6-and-0-7-with/ta-p/225724

* http://community.microstrategy.com/t5/Release-Notes/TN231092-Certified-Database-and-ODBC-configurations-for/ta-p/231092

* http://community.microstrategy.com/t5/Release-Notes/TN231094-Certified-Database-and-ODBC-configurations-for/ta-p/231094   

