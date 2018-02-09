---
title: "Configuring JReport with Drill"
date: 2018-02-09 00:16:03 UTC
parent: "Using Drill with BI Tools"
---

JReport is an embeddable BI solution that empowers users to analyze data and create reports and dashboards. JReport accesses data from Hadoop systems through Apache Drill. By visualizing data through Drill, users can perform their own reporting and data discovery for agile, on-the-fly decision-making.

You can use JReport 13.1 and the Apache Drill JDBC Driver to easily extract data and visualize it, creating reports and dashboards that you can embed into your own applications. Complete the following simple steps to use Apache Drill with JReport:

1. Install the Drill JDBC Driver with JReport.
2. Create a new JReport Catalog to manage the Drill connection.
3. Use JReport Designer to query the data and create a report.


## Step 1: Install the Drill JDBC Driver with JReport

Drill provides standard JDBC connectivity to integrate with JReport. JReport 13.1 requires Drill 1.0 or later.
For general instructions on installing the Drill JDBC driver, see [Using JDBC]({{ site.baseurl }}/docs/using-the-jdbc-driver/).

1. Locate the JDBC driver in the Drill installation directory on any node where Drill is installed on the cluster: 
        <drill-home>/jars/jdbc-driver/drill-jdbc-all-<drill-version>.jar 
   
2. Copy the Drill JDBC driver into the JReport `lib` folder:
        %REPORTHOME%\lib\
   For example, on Windows, copy the Drill JDBC driver jar file into:
   
        C:\JReport\Designer\lib\drill-jdbc-all-1.0.0.jar
    
3.  Add the location of the JAR file to the JReport CLASSPATH variable. On Windows, edit the `C:\JReport\Designer\bin\setenv.bat` file:
    ![drill query flow]({{ site.baseurl }}/docs/img/jreport_setenv.png)

4. Verify that the JReport system can resolve the hostnames of the ZooKeeper nodes of the Drill cluster. You can do this by configuring DNS for all of the systems. Alternatively, you can edit the hosts file on the JReport system to include the hostnames and IP addresses of all the ZooKeeper nodes used with the Drill cluster.  For Linux systems, the hosts file is located at `/etc/hosts`. For Windows systems, the hosts file is located at `%WINDIR%\system32\drivers\etc\hosts`  Here is an example of a Windows hosts file: ![drill query flow]({{ site.baseurl }}/docs/img/jreport-hostsfile.png)


## Step 2: Create a New JReport Catalog to Manage the Drill Connection

1.  Click Create **New -> Catalog…**
2.  Provide a catalog file name and click **…** to choose the file-saving location.
3.  Click **View -> Catalog Browser**.
4.  Right-click **Data Source 1** and select **Add JDBC Connection**.
5.  Fill in the **Driver**, **URL**, **User**, and **Password** fields. ![drill query flow]({{ site.baseurl }}/docs/img/jreport-catalogbrowser.png)
6.  Click **Options** and select the **Qualifier** tab. 
7.  In the **Quote Qualifier** section, choose **User Defined** and change the quote character from “ to ` (backtick). ![drill query flow]({{ site.baseurl }}/docs/img/jreport-quotequalifier.png)
8.  Click **OK**. JReport will verify the connection and save all information.
9.  Add tables and views to the JReport catalog by right-clicking the connection node and choosing **Add Table**. Now you can browse the schemas and add specific tables that you want to make available for building queries. ![drill query flow]({{ site.baseurl }}/docs/img/jreport-addtable.png)
10. Click **Done** when you have added all the tables you need. 


## Step 3: Use JReport Designer

1.  In the Catalog Browser, right-click **Queries** and select **Add Query…**
2.  Define a JReport query by using the Query Editor. You can also import your own SQL statements. ![drill query flow]({{ site.baseurl }}/docs/img/jreport-queryeditor.png)
3.  Click **OK** to close the Query Editor, and click the **Save Catalog** button to save your progress to the catalog file. 
    **Note**: If the report returns errors, you may need to edit the query and add the schema in front of the table name: `select column from schema.table_name` You can do this by clicking the **SQL** button on the Query Editor.

4.  Use JReport Designer to query the data and create a report. ![drill query flow]({{ site.baseurl }}/docs/img/jreport-crosstab.png)
    ![drill query flow]({{ site.baseurl }}/docs/img/jreport-crosstab2.png)
    ![drill query flow]({{ site.baseurl }}/docs/img/jreport-crosstab3.png)
