---
title: "Drill in 10 Minutes"
parent: "Tutorials"
description: Get started with Drill in 10 minutes or less.
---
## Objective

Use Apache Drill to query sample data in 10 minutes. For simplicity, you’ll
run Drill in _embedded_ mode rather than _distributed_ mode to try out Drill
without having to perform any setup tasks.

## A Few Bits About Apache Drill

Drill is a clustered, powerful MPP (Massively Parallel Processing) query
engine for Hadoop that can process petabytes of data, fast. Drill is useful
for short, interactive ad-hoc queries on large-scale data sets. Drill is
capable of querying nested data in formats like JSON and Parquet and
performing dynamic schema discovery. Drill does not require a centralized
metadata repository.

### **_Dynamic schema discovery_**

Drill does not require schema or type specification for data in order to start
the query execution process. Drill starts data processing in record-batches
and discovers the schema during processing. Self-describing data formats such
as Parquet, JSON, AVRO, and NoSQL databases have schema specified as part of
the data itself, which Drill leverages dynamically at query time. Because
schema can change over the course of a Drill query, all Drill operators are
designed to reconfigure themselves when schemas change.

### **_Flexible data model_**

Drill allows access to nested data attributes, just like SQL columns, and
provides intuitive extensions to easily operate on them. From an architectural
point of view, Drill provides a flexible hierarchical columnar data model that
can represent complex, highly dynamic and evolving data models. Drill allows
for efficient processing of these models without the need to flatten or
materialize them at design time or at execution time. Relational data in Drill
is treated as a special or simplified case of complex/multi-structured data.

### **_De-centralized metadata_**

Drill does not have a centralized metadata requirement. You do not need to
create and manage tables and views in a metadata repository, or rely on a
database administrator group for such a function. Drill metadata is derived
from the storage plugins that correspond to data sources. Storage plugins
provide a spectrum of metadata ranging from full metadata (Hive), partial
metadata (HBase), or no central metadata (files). De-centralized metadata
means that Drill is NOT tied to a single Hive repository. You can query
multiple Hive repositories at once and then combine the data with information
from HBase tables or with a file in a distributed file system. You can also
use SQL DDL syntax to create metadata within Drill, which gets organized just
like a traditional database. Drill metadata is accessible through the ANSI
standard INFORMATION_SCHEMA database.

### **_Extensibility_**

Drill provides an extensible architecture at all layers, including the storage
plugin, query, query optimization/execution, and client API layers. You can
customize any layer for the specific needs of an organization or you can
extend the layer to a broader array of use cases. Drill provides a built in
classpath scanning and plugin concept to add additional storage plugins,
functions, and operators with minimal configuration.

## Process Overview

Download the Apache Drill archive and extract the contents to a directory on
your machine. The Apache Drill archive contains sample JSON and Parquet files
that you can query immediately.

Query the sample JSON and parquet files using SQLLine. SQLLine is a pure-Java
console-based utility for connecting to relational databases and executing SQL
commands. SQLLine is used as the shell for Drill. Drill follows the ANSI SQL:
2011 standard with a few extensions for nested data formats.

### Prerequisite

You must have the following software installed on your machine to run Drill:

<table ><tbody><tr><td ><strong>Software</strong></td><td ><strong>Description</strong></td></tr><tr><td ><a href="http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html" class="external-link" rel="nofollow">Oracle JDK version 7</a></td><td >A set of programming tools for developing Java applications.</td></tr></tbody></table>

  
### Prerequisite Validation

Run the following command to verify that the system meets the software
prerequisite:
<table ><tbody><tr><td ><strong>Command </strong></td><td ><strong>Example Output</strong></td></tr><tr><td ><code>java –version</code></td><td ><code>java version &quot;1.7.0_65&quot;</code><br /><code>Java(TM) SE Runtime Environment (build 1.7.0_65-b19)</code><br /><code>Java HotSpot(TM) 64-Bit Server VM (build 24.65-b04, mixed mode)</code></td></tr></tbody></table>
  
## Install Drill

You can install Drill on a machine running Linux, Mac OS X, or Windows.  

### Installing Drill on Linux

Complete the following steps to install Drill:

  1. Issue the following command to download the latest, stable version of Apache Drill to a directory on your machine:
        
        wget http://getdrill.org/drill/download/apache-drill-0.8.0.tar.gz
  2. Issue the following command to create a new directory to which you can extract the contents of the Drill `tar.gz` file:
  
        sudo mkdir -p /opt/drill
  3. Navigate to the directory where you downloaded the Drill `tar.gz` file.
  4. Issue the following command to extract the contents of the Drill `tar.gz` file:
  
        sudo tar -xvzf apache-drill-<version>.tar.gz -C /opt/drill
  5. Issue the following command to navigate to the Drill installation directory:
  
        cd /opt/drill/apache-drill-<version>

At this point, you can [start Drill]({{ site.baseurl }}/docs/drill-in-10-minutes/#start-drill).

### Installing Drill on Mac OS X

Complete the following steps to install Drill:

  1. Open a Terminal window, and create a `drill` directory inside your home directory (or in some other location if you prefer).
  
     **Example**

        $ pwd
        /Users/max
        $ mkdir drill
        $ cd drill
        $ pwd
        /Users/max/drill
  2. Click the following link to download the latest, stable version of Apache Drill:  
      [http://getdrill.org/drill/download/apache-drill-0.8.0.tar.gz](http://getdrill.org/drill/download/apache-drill-0.8.0.tar.gz)
  3. Open the downloaded `TAR` file with the Mac Archive utility or a similar tool for unzipping files.
  4. Move the resulting `apache-drill-<version>` folder into the `drill` directory that you created.
  5. Issue the following command to navigate to the `apache-drill-<version>` directory:
  
        cd /Users/max/drill/apache-drill-<version>

At this point, you can [start Drill](/docs/drill-in-10-minutes/#start-drill).

### Installing Drill on Windows

You can install Drill on Windows 7 or 8. To install Drill on Windows, you must
have JDK 7, and you must set the `JAVA_HOME` path in the Windows Environment
Variables. You must also have a utility, such as
[7-zip](http://www.7-zip.org/), installed on your machine. These instructions
assume that the [7-zip](http://www.7-zip.org/) decompression utility is
installed to extract a Drill archive file that you download.

#### Setting JAVA_HOME

Complete the following steps to set `JAVA_HOME`:

  1. Navigate to `Control Panel\All Control Panel Items\System`, and select **Advanced System Settings**. The System Properties window appears.
  2. On the Advanced tab, click **Environment Variables**. The Environment Variables window appears.
  3. Add/Edit `JAVA_HOME` to point to the location where the JDK software is located.
  
       **Example**
       
        C:\Program Files\Java\jdk1.7.0_65
  4. Click **OK** to exit the windows.

#### Installing Drill

Complete the following steps to install Drill:

  1. Create a `drill` directory on your `C:\` drive, (or in some other location if you prefer).
  
       **Example**
       
         C:\drill
     Do not include spaces in your directory path. If you include spaces in the
directory path, Drill fails to run.
  2. Click the following link to download the latest, stable version of Apache Drill: 
      [http://getdrill.org/drill/download/apache-drill-0.8.0.tar.gz](http://getdrill.org/drill/download/apache-drill-0.8.0.tar.gz)
  3. Move the `apache-drill-<version>.tar.gz` file to the `drill` directory that you created on your `C:\` drive.
  4. Unzip the `TAR.GZ` file and the resulting `TAR` file.
     1. Right-click `apache-drill-<version>.tar.gz,` and select `7-Zip>Extract Here`. The utility extracts the `apache-drill-<version>.tar` file.
     2. Right-click `apache-drill-<version>.tar`, and select ` 7-Zip>Extract Here`. The utility extracts the `apache-drill-<version> `folder.
  5. Open the `apache-drill-<version>` folder.
  6. Open the `bin` folder, and double-click on the `sqlline.bat` file. The Windows command prompt opens.
  7. At the `sqlline>` prompt, type `!connect jdbc:drill:zk=local` and then press `Enter`.
  8. Enter the username and password.
     1. When prompted, enter the user name `admin` and then press Enter.
     2. When prompted, enter the password `admin` and then press Enter. The cursor blinks for a few seconds and then `0: jdbc:drill:zk=local>` displays in the prompt.

At this point, you can submit queries to Drill. Refer to the [Query Sample Data](/docs/drill-in-10-minutes#query-sample-data) section of this document.

## Start Drill

Launch SQLLine, the Drill shell, to start and run Drill in embedded mode.
Launching SQLLine automatically starts a new Drillbit within the shell. In a
production environment, Drillbits are the daemon processes that run on each
node in a Drill cluster.

Complete the following steps to launch SQLLine and start Drill:

  1. Verify that you are in the Drill installation directory.  
Example: `~/apache-drill-<version>`

  2. Issue the following command to launch SQLLine:

        bin/sqlline -u jdbc:drill:zk=local

     `-u` is a JDBC connection string that directs SQLLine to connect to Drill. It
also starts a local Drillbit. If you are connecting to an Apache Drill
cluster, the value of `zk=` would be a list of Zookeeper quorum nodes. For
more information about how to run Drill in clustered mode, go to [Deploying Drill in a Cluster](/docs/deploying-drill-in-a-cluster).

When SQLLine starts, the system displays the following prompt:  
`0: jdbc:drill:zk=local>`

Issue the following command when you want to exit SQLLine:

    !quit


## Query Sample Data

Your Drill installation includes a `sample-date` directory with JSON and
Parquet files that you can query. The local file system on your machine is
configured as the `dfs` storage plugin instance by default when you install
Drill in embedded mode. For more information about storage plugin
configuration, refer to [Storage Plugin Registration](/docs/connect-a-data-source-introduction).

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

**Note:** When you enter the query, include the version of Drill that you are currently running. 

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

  * [Deploy Drill in a clustered environment.](/docs/deploying-drill-in-a-cluster)
  * [Configure storage plugins to connect Drill to your data sources](/docs/connect-a-data-source-introduction).
  * Query [Hive](/docs/querying-hive) and [HBase](/docs/hbase-storage-plugin) data.
  * [Query Complex Data](/docs/querying-complex-data)
  * [Query Plain Text Files](/docs/querying-plain-text-files)

## More Information

For more information about Apache Drill, explore the  [Apache Drill
web site](http://drill.apache.org).