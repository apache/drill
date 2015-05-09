---
title: "Drill in 10 Minutes"
parent: "Tutorials"
description: Get started with Drill in 10 minutes or less.
---
## Objective

Use Apache Drill to query sample data in 10 minutes. For simplicity, you’ll
run Drill in _embedded_ mode rather than _distributed_ mode to try out Drill
without having to perform any setup tasks.

## Installation Overview

You can install Drill in embedded mode on a machine running Linux, Mac OS X, or Windows. For information about running Drill in distributed mode, see  [Deploying Drill in a Cluster]({{ site.baseurl }}/docs/deploying-drill-in-a-cluster).

This installation procedure includes how to download the Apache Drill archive and extract the contents to a directory on your machine. The Apache Drill archive contains sample JSON and Parquet files that you can query immediately.

After installing Drill, you start  SQLLine. SQLLine is a pure-Java console-based utility for connecting to relational databases and executing SQL commands. SQLLine is used as the shell for Drill. Drill follows the ANSI SQL: 2011 standard with [extensions]({{site.baseurl}}/docs/sql-extensions/) for nested data formats and other capabilities.

## Embedded Mode Installation Prerequisites

You need to meet the following prerequisites to run Drill:

* Linux, Mac OS X, and Windows: [Oracle Java SE Development (JDK) Kit 7](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html) installation  
* Windows only:  
  * A JAVA_HOME environment variable set up that points to  to the JDK installation  
  * A PATH environment variable that includes a pointer to the JDK installation  
  * A third-party utility for unzipping a tar.gz file 
  
### Java Installation Prerequisite Check

Run the following command in a terminal (Linux and Mac OS X) or Command Prompt (Windows) to verify that Java 7 is the version in effect:

    java -version

The output looks something like this:

    java version "1.7.0_79"
    Java(TM) SE Runtime Environment (build 1.7.0_7965-b15)
    Java HotSpot(TM) 64-Bit Server VM (build 24.79-b02, mixed mode)

## Install Drill on Linux or Mac OS X

Complete the following steps to install Drill:  

1. Issue the following command in a terminal to download the latest, stable version of Apache Drill to a directory on your machine, or download Drill from the [Drill web site](http://getdrill.org/drill/download/apache-drill-0.9.0.tar.gz):

        wget http://getdrill.org/drill/download/apache-drill-0.9.0.tar.gz  

2. Copy the downloaded file to the directory where you want to install Drill. 

3. Extract the contents of the Drill tar.gz file. Use sudo if necessary:  

        sudo tar -xvzf apache-drill-0.9.0..tar.gz  

The extraction process creates the installation directory named apache-drill-0.9.0 containing the Drill software.

At this point, you can [start Drill]({{site.baseurl}}/docs/drill-in-10-minutes/#start-drill).

## Start Drill on Linux and Mac OS X
Launch SQLLine using the sqlline command to start to Drill in embedded mode. The command directs SQLLine to connect to Drill. The zk=local means the local node is the ZooKeeper node. Complete the following steps to launch SQLLine and start Drill:

1. Navigate to the Drill installation directory. For example:  

        cd apache-drill-0.9.0  

2. Issue the following command to launch SQLLine:

        bin/sqlline -u jdbc:drill:zk=local  

   The `0: jdbc:drill:zk=local>`  prompt appears.  

   At this point, you can [submit queries]({{site.baseurl}}/docs/drill-in-10-minutes#query-sample-data) to Drill.

### Install Drill on Windows

You can install Drill on Windows 7 or 8. First, set the JAVA_HOME environment variable, and then install Drill. Complete the following steps to install Drill:

1. Click the following link to download the latest, stable version of Apache Drill:  [http://getdrill.org/drill/download/apache-drill-0.9.0.tar.gz](http://getdrill.org/drill/download/apache-drill-0.9.0.tar.gz)
2. Move the `apache-drill-0.9.0.tar.gz` file to a directory where you want to install Drill.
3. Unzip the `TAR.GZ` file using a third-party tool. If the tool you use does not unzip the TAR file as well as the `TAR.GZ` file, unzip the `apache-drill-0.9.0.tar` to extract the Drill software. The extraction process creates the installation directory named apache-drill-0.9.0 containing the Drill software. For example:
   ![drill install dir]({{ site.baseurl }}/docs/img/drill-directory.png)
   At this point, you can start Drill.  

## Start Drill on Windows
Launch SQLLine using the **sqlline command** to start to Drill in embedded mode. The command directs SQLLine to connect to Drill. The `zk=local` means the local node is the ZooKeeper node. Complete the following steps to launch SQLLine and start Drill:

1. Open the apache-drill-0.9.0 folder.  
2. Open the bin folder, and double-click the `sqlline.bat` file:
   ![drill bin dir]({{ site.baseurl }}/docs/img/drill-bin.png)
   The Windows command prompt opens.  
3. At the sqlline> prompt, type `!connect jdbc:drill:zk=local` and then press Enter:
   ![sqlline]({{ site.baseurl }}/docs/img/sqlline1.png)
4. Enter the username, `admin`, and password, also `admin` when prompted.
   The `0: jdbc:drill:zk=local>` prompt appears.
At this point, you can [submit queries]({{ site.baseurl }}/docs/drill-in-10-minutes#query-sample-data) to Drill.

## Stopping Drill

Issue the following command when you want to exit SQLLine:

    !quit

## Query Sample Data

Your Drill installation includes a `sample-date` directory with JSON and
Parquet files that you can query. The local file system on your machine is
configured as the `dfs` storage plugin instance by default when you install
Drill in embedded mode. For more information about storage plugin
configuration, refer to [Storage Plugin Registration]({{ site.baseurl }}/docs/connect-a-data-source-introduction).

Use SQL syntax to query the sample `JSON` and `Parquet` files in the `sample-data` directory on your local file system.

### Querying a JSON File

A sample JSON file, `employee.json`, contains fictitious employee data.

To view the data in the `employee.json` file, submit the following SQL query
to Drill:
    
    0: jdbc:drill:zk=local> SELECT * FROM cp.`employee.json`;

The query returns the following results:

**Example of partial output**

    +-------------+------------+------------+------------+-------------+-----------+
    | employee_id | full_name  | first_name | last_name  | position_id | position_ |
    +-------------+------------+------------+------------+-------------+-----------+
    | 1101        | Steve Eurich | Steve      | Eurich         | 16          | Store T |
    | 1102        | Mary Pierson | Mary       | Pierson    | 16          | Store T |
    | 1103        | Leo Jones  | Leo        | Jones      | 16          | Store Tem |
    | 1104        | Nancy Beatty | Nancy      | Beatty     | 16          | Store T |
    | 1105        | Clara McNight | Clara      | McNight    | 16          | Store  |
    | 1106        | Marcella Isaacs | Marcella   | Isaacs     | 17          | Stor |
    | 1107        | Charlotte Yonce | Charlotte  | Yonce      | 17          | Stor |
    | 1108        | Benjamin Foster | Benjamin   | Foster     | 17          | Stor |
    | 1109        | John Reed  | John       | Reed       | 17          | Store Per |
    | 1110        | Lynn Kwiatkowski | Lynn       | Kwiatkowski | 17          | St |
    | 1111        | Donald Vann | Donald     | Vann       | 17          | Store Pe |
    | 1112        | William Smith | William    | Smith      | 17          | Store  |
    | 1113        | Amy Hensley | Amy        | Hensley    | 17          | Store Pe |
    | 1114        | Judy Owens | Judy       | Owens      | 17          | Store Per |
    | 1115        | Frederick Castillo | Frederick  | Castillo   | 17          | S |
    | 1116        | Phil Munoz | Phil       | Munoz      | 17          | Store Per |
    | 1117        | Lori Lightfoot | Lori       | Lightfoot  | 17          | Store |
    +-------------+------------+------------+------------+-------------+-----------+
    1,155 rows selected (0.762 seconds)
    0: jdbc:drill:zk=local>

### Querying a Parquet File

Query the `region.parquet` and `nation.parquet` files in the `sample-data`
directory on your local file system.

#### Region File

If you followed the Apache Drill in 10 Minutes instructions to install Drill
in embedded mode, the path to the parquet file varies between operating
systems.

{% include startnote.html %}When you enter the query, include the version of Drill that you are currently running.{% include endnote.html %} 

To view the data in the `region.parquet` file, issue the query appropriate for
your operating system:

* Linux  

        SELECT * FROM dfs.`/opt/drill/apache-drill-<version>/sample-data/region.parquet`;
* Mac OS X
  
        SELECT * FROM dfs.`/Users/max/drill/apache-drill-<version>/sample-data/region.parquet`;
* Windows  
        
        SELECT * FROM dfs.`C:\drill\apache-drill-<version>\sample-data\region.parquet`;

The query returns the following results:

    +------------+------------+
    |   EXPR$0   |   EXPR$1   |
    +------------+------------+
    | AFRICA     | lar deposits. blithely final packages cajole. regular waters ar |
    | AMERICA    | hs use ironic, even requests. s |
    | ASIA       | ges. thinly even pinto beans ca |
    | EUROPE     | ly final courts cajole furiously final excuse |
    | MIDDLE EAST | uickly special accounts cajole carefully blithely close reques |
    +------------+------------+
    5 rows selected (0.165 seconds)
    0: jdbc:drill:zk=local>

#### Nation File

If you followed the Apache Drill in 10 Minutes instructions to install Drill
in embedded mode, the path to the parquet file varies between operating
systems.

**Note:** When you enter the query, include the version of Drill that you are currently running. 

To view the data in the `nation.parquet` file, issue the query appropriate for
your operating system:

* Linux  

          SELECT * FROM dfs.`/opt/drill/apache-drill-<version>/sample-data/nation.parquet`;
* Mac OS X
  
          SELECT * FROM dfs.`/Users/max/drill/apache-drill-<version>/sample-data/nation.parquet`;

* Windows 
 
          SELECT * FROM dfs.`C:\drill\apache-drill-<version>\sample-data\nation.parquet`;

The query returns the following results:

## Summary

Now you know a bit about Apache Drill. To summarize, you have completed the
following tasks:

  * Learned that Apache Drill supports nested data, schema-less execution, and decentralized metadata.
  * Downloaded and installed Apache Drill.
  * Invoked SQLLine with Drill in embedded mode.
  * Queried the sample JSON file, `employee.json`, to view its data.
  * Queried the sample `region.parquet` file to view its data.
  * Queried the sample `nation.parquet` file to view its data.

## Next Steps

Now that you have an idea about what Drill can do, you might want to:

  * [Deploy Drill in a clustered environment.]({{ site.baseurl }}/docs/deploying-drill-in-a-cluster)
  * [Configure storage plugins to connect Drill to your data sources]({{ site.baseurl }}/docs/connect-a-data-source-introduction).
  * Query [Hive]({{ site.baseurl }}/docs/querying-hive) and [HBase]({{ site.baseurl }}/docs/hbase-storage-plugin) data.
  * [Query Complex Data]({{ site.baseurl }}/docs/querying-complex-data)
  * [Query Plain Text Files]({{ site.baseurl }}/docs/querying-plain-text-files)

## More Information

For more information about Apache Drill, explore the  [Apache Drill
web site](http://drill.apache.org).