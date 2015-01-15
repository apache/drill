---
title: "Using JDBC to Access Apache Drill from SQuirreL"
parent: "Drill Interfaces"
---
You can connect to Drill through a JDBC client tool, such as SQuirreL, on
Windows, Linux, and Mac OS X systems, to access all of your data sources
registered with Drill. An embedded JDBC driver is included with Drill.
Configure the JDBC driver in the SQuirreL client to connect to Drill from
SQuirreL. This document provides instruction for connecting to Drill from
SQuirreL on Windows.

To use the Drill JDBC driver with SQuirreL on Windows, complete the following
steps:

  * Step 1: Getting the Drill JDBC Driver 
  * Step 2: Installing and Starting SQuirreL
  * Step 3: Adding the Drill JDBC Driver to SQuirreL
  * Step 4: Running a Drill Query from SQuirreL

For information about how to use SQuirreL, refer to the [SQuirreL Quick
Start](http://squirrel-sql.sourceforge.net/user-manual/quick_start.html)
guide.

### Prerequisites

  * SQuirreL requires JRE 7
  * Drill installed in distributed mode on one or multiple nodes in a cluster. Refer to the [Install Drill](https://cwiki.apache.org/confluence/display/DRILL/Install+Drill) documentation for more information.
  * The client must be able to resolve the actual hostname of the Drill node(s) with the IP(s). Verify that a DNS entry was created on the client machine for the Drill node(s).   
If a DNS entry does not exist, create the entry for the Drill node(s).

    * For Windows, create the entry in the %WINDIR%\system32\drivers\etc\hosts file.

    * For Linux and Mac, create the entry in /etc/hosts.  
<drill-machine-IP> <drill-machine-hostname>  
Example: `127.0.1.1 maprdemo`

## Step 1: Getting the Drill JDBC Driver

The Drill JDBC Driver `JAR` file must exist in a directory on your Windows
machine in order to configure the driver in the SQuirreL client.

You can copy the Drill JDBC `JAR` file from the following Drill installation
directory on the node with Drill installed, to a directory on your Windows
machine:

    <drill_installation_directory>/jars/jdbc-driver/drill-jdbc-all-0.7.0-SNAPSHOT.jar

Or, you can download the [apache-
drill-0.7.0.tar.gz](http://www.apache.org/dyn/closer.cgi/drill/drill-0.7.0
/apache-drill-0.7.0.tar.gz) file to a location on your Windows machine, and
extract the contents of the file. You may need to use a decompression utility,
such as [7-zip](http://www.7-zip.org/) to extract the archive. Once extracted,
you can locate the driver in the following directory:

    <windows_directory>\apache-drill-<version>\jars\jdbc-driver\drill-jdbc-all-0.7.0-SNAPSHOT.jar

## Step 2: Installing and Starting SQuirreL

To install and start SQuirreL, complete the following steps:

  1. Download the SQuirreL JAR file for Windows from the following location:  
<http://www.squirrelsql.org/#installation>

  2. Double-click the SQuirreL `JAR` file. The SQuirreL installation wizard walks you through the installation process.
  3. When installation completes, navigate to the SQuirreL installation folder and then double-click `squirrel-sql.bat` to start SQuirreL.

## Step 3: Adding the Drill JDBC Driver to SQuirreL

To add the Drill JDBC Driver to SQuirreL, define the driver and create a
database alias. The alias is a specific instance of the driver configuration.
SQuirreL uses the driver definition and alias to connect to Drill so you can
access data sources that you have registered with Drill.

### A. Define the Driver

To define the Drill JDBC Driver, complete the following steps:

  1. In the SQuirreL toolbar, select **Drivers > New Driver**. The Add Driver dialog box appears.
  
  ![](../../../img/40.png)
     
  2. Enter the following information:

     <table class="confluenceTable"><tbody><tr><td valign="top"><p><strong>Option</strong></p></td><td valign="top"><p><strong>Description</strong></p></td></tr><tr><td valign="top"><p>Name</p></td><td valign="top"><p>Name for the Drill JDBC Driver</p></td></tr><tr><td valign="top"><p>Example URL</p></td><td valign="top"><p><code>jdbc:drill:zk=&lt;<em>zookeeper_quorum</em>&gt;[;schema=&lt;<em>schema_to_use_as_default</em>&gt;]</code></p><p><strong>Example:</strong><code> jdbc:drill:zk=maprdemo:5181</code></p><p><strong>Note:</strong> The default ZooKeeper port is 2181. In a MapR cluster, the ZooKeeper port is 5181.</p></td></tr><tr><td valign="top"><p>Website URL</p></td><td valign="top"><p><code>jdbc:drill:zk=&lt;<em>zookeeper_quorum</em>&gt;[;schema=&lt;<em>schema_to_use_as_default</em>&gt;]</code></p><p><strong>Example:</strong><code><code> jdbc:drill:zk=maprdemo:5181</code></code></p><p><strong>Note:</strong><span> The default ZooKeeper port is 2181. In a MapR cluster, the ZooKeeper port is 5181.</span></p></td></tr><tr><td valign="top"><p>Extra Class Path</p></td><td valign="top"><p>Click <strong>Add</strong> and navigate to the JDBC <code>JAR</code> file location in the Windows directory:<br /><code>&lt;windows_directory&gt;/jars/jdbc-driver/<span style="color: rgb(34,34,34);">drill-jdbc-all-0.6.0-</span><span style="color: rgb(34,34,34);">incubating.jar</span></code></p><p>Select the <code>JAR</code> file, click <strong>Open</strong>, and then click <strong>List Drivers</strong>.</p></td></tr><tr><td valign="top"><p>Class Name</p></td><td valign="top"><p>Select <code>org.apache.drill.jdbc.Driver</code> from the drop-down menu.</p></td></tr></tbody></table>  
  
  3. Click **OK**. The SQuirreL client displays a message stating that the driver registration is successful, and you can see the driver in the Drivers panel.  

     ![](../../../img/52.png)

### B. Create an Alias

To create an alias, complete the following steps:

  1. Select the **Aliases** tab.
  2. In the SQuirreL toolbar, select **Aliases >****New Alias**. The Add Alias dialog box appears.
    
     ![](../../../img/19.png)

  3. Enter the following information:
  
     <table class="confluenceTable"><tbody><tr><td valign="top"><p><strong>Option</strong></p></td><td valign="top"><p><strong>Description</strong></p></td></tr><tr><td valign="top"><p>Alias Name</p></td><td valign="top"><p>A unique name for the Drill JDBC Driver alias.</p></td></tr><tr><td valign="top"><p>Driver</p></td><td valign="top"><p>Select the Drill JDBC Driver.</p></td></tr><tr><td valign="top"><p>URL</p></td><td valign="top"><p>Enter the connection URL with <span>the name of the Drill directory stored in ZooKeeper and the cluster ID:</span></p><p><code>jdbc:drill:zk=&lt;<em>zookeeper_quorum</em>&gt;/&lt;drill_directory_in_zookeeper&gt;/&lt;cluster_ID&gt;;schema=&lt;<em>schema_to_use_as_default</em>&gt;</code></p><p><strong>The following examples show URLs for Drill installed on a single node:</strong><br /><span style="font-family: monospace;font-size: 14.0px;line-height: 1.4285715;background-color: transparent;">jdbc:drill:zk=10.10.100.56:5181/drill/demo_mapr_com-drillbits;schema=hive<br /></span><span style="font-family: monospace;font-size: 14.0px;line-height: 1.4285715;background-color: transparent;">jdbc:drill:zk=10.10.100.24:2181/drill/drillbits1;schema=hive<br /> </span></p><div><strong>The following example shows a URL for Drill installed in distributed mode with a connection to a ZooKeeper quorum:</strong></div><div><span style="font-family: monospace;font-size: 14.0px;line-height: 1.4285715;background-color: transparent;">jdbc:drill:zk=10.10.100.30:5181,10.10.100.31:5181,10.10.100.32:5181/drill/drillbits1;schema=hive</span></div>    <div class="aui-message warning shadowed information-macro">
                            <span class="aui-icon icon-warning"></span>
                <div class="message-content">
                            <ul><li style="list-style-type: none;background-image: none;"><ul><li>Including a default schema is optional.</li><li>The ZooKeeper port is 2181. In a MapR cluster, the ZooKeeper port is 5181.</li><li>The Drill directory stored in ZooKeeper is <code>/drill</code>. </li><li>The Drill default cluster ID is<code> drillbits1</code>.</li></ul></li></ul>
                    </div>
    </div>
</td></tr><tr><td valign="top"><p>User Name</p></td><td valign="top"><p>admin</p></td></tr><tr><td valign="top"><p>Password</p></td><td valign="top"><p>admin</p></td></tr></tbody></table>

  
  4. Click **Ok. **The Connect to: dialog box appears.  

     ![](../../../img/30.png?version=1&modificationDate=1410385290359&api=v2)

  5. Click **Connect.** SQuirreL displays a message stating that the connection is successful.  
![](../../../img/53.png?version=1&modificationDate=1410385313418&api=v2)

  6. Click **OK**. SQuirreL displays a series of tabs.

## Step 4: Running a Drill Query from SQuirreL

Once you have SQuirreL successfully connected to your cluster through the
Drill JDBC Driver, you can issue queries from the SQuirreL client. You can run
a test query on some sample data included in the Drill installation to try out
SQuirreL with Drill.

To query sample data with Squirrel, complete the following steps:

  1. Click the ![](http://doc.mapr.com/download/attachments/26986731/image2014-9-10%2014%3A43%3A14.png?version=1&modificationDate=1410385394576&api=v2) tab.
  2. Enter the following query in the query box:   
``SELECT * FROM cp.`employee.json`;``  
Example:  
 ![](../../../img/11.png?version=1&modificationDate=1410385451811&api=v2)

  3. Press **Ctrl+Enter** to run the query. The following query results display:  
 ![](../../../img/42.png?version=1&modificationDate=1410385482574&api=v2)

You have successfully run a Drill query from the SQuirreL client.

