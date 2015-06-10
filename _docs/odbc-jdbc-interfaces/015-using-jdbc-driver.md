---
title: "Using the JDBC Driver"
parent: "ODBC/JDBC Interfaces"
---
This section explains how to install and use the JDBC driver for Apache Drill. For specific examples of client tool connections to Drill via JDBC, see [Using JDBC with SQuirreL]({{ site.baseurl }}/docs/.../) and [Configuring Spotfire Server]({{ site.baseurl }}/docs/.../).


### Prerequisites

  * JRE 7 or JDK 7
  * Drill installed either in embedded mode or in distributed mode on one or more nodes in a cluster. Refer to the [Install Drill]({{ site.baseurl }}/docs/install-drill/) documentation for more information.
  * The client must be able to resolve the actual hostname of the Drill node(s) with the IP(s). Verify that a DNS entry was created on the client machine for the Drill node(s).
     
If a DNS entry does not exist, create the entry for the Drill node(s).

    * For Windows, create the entry in the %WINDIR%\system32\drivers\etc\hosts file.
    * For Linux and Mac OSX, create the entry in /etc/hosts.  
<drill-machine-IP> <drill-machine-hostname>
    Example: `127.0.1.1 maprdemo`


----------

### Getting the Drill JDBC Driver

The Drill JDBC Driver `JAR` file must exist in a directory on a client machine so you can configure the driver for the application or third-party tool that you intend to use. You can obtain the driver in two different ways:

1. Copy the `drill-jdbc-all` JAR file from the following Drill installation directory on a node where Drill is installed to a directory on your client
machine:

    <drill_installation_directory>/jars/jdbc-driver/drill-jdbc-all-<version>.jar
    
    For example: drill1.0/jdbc-driver/drill-jdbc-all-1.0.0-mapr-r1.jar

2. Download the following tar file to a location on your client machine: [apache-
drill-1.0.0.tar.gz](http://apache.osuosl.org/drill/drill-1.0.0/apache-drill-1.0.0-src.tar.gz) and extract the file. You may need to use a decompression utility, such as [7-zip](http://www.7-zip.org/). The driver is extracted to the following directory:

    <drill-home>\apache-drill-<version>\jars\jdbc-driver\drill-jdbc-all-<version>.jar

Mac vs windows paths here....

On a MapR cluster, the JDBC driver is installed here: `/opt/mapr/drill/drill-1.0.0/jars/jdbc-driver/`

----------

### JDBC Driver URLs

To configure a JDBC application, users have to:

1. Put the Drill JDBC jar file on the class path.
2. Use a valid Drill JDBC URL.
3. Configure tools or application code with the name of the Drill driver class.

The driver URLs that you use to create JDBC connection strings must be formed as stated in the following sections. 


#### Driver Class Name

The class name for the JDBC driver is `org.apache.drill.jdbc.Driver`

#### URL Syntax

The form of the driver's JDBC URLs is as follows. The URL consists of some required and some optional parameters. 

A Drill JDBC URL must start with: `"{{jdbc:drill:}}"`

#### URL Examples

`jdbc:drill:zk=maprdemo:5181`

where `zk=maprdemo:5181` defines the ZooKeeper quorum.

`jdbc:drill:zk=10.10.100.56:5181/drill/drillbits1;schema=hive`

where the ZooKeeper node IP address is provided as well as the Drill directory in ZK and the cluster ID?

`jdbc:drill:zk=10.10.100.30:5181,10.10.100.31:5181,10.10.100.32:5181/drill/drillbits1;schema=hive`

<li>Including a default schema is optional.</li>
<li>The ZooKeeper port is 2181. In a MapR cluster, the ZooKeeper port is 5181.</li>
<li>The Drill directory stored in ZooKeeper is <code>/drill</code>.</li>
<li>The Drill default cluster ID is<code> drillbits1</code>.</li>

---------

### JDBC Driver Configuration Options

To control the behavior of the Drill JDBC driver, you can append the following configuration options to the JDBC URL:

<config options>


----------


### Related Documentation

When you have connected to Drill through the JDBC Driver, you can issue queries from the JDBC application or client. Start by running
a test query on some sample data included in the Drill installation.

