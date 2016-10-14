---
title: "Using the JDBC Driver"
date:  
parent: "ODBC/JDBC Interfaces"
---
This section explains how to install and use the JDBC driver for Apache Drill. To use the JDBC driver, you have to:

1. [Meet prerequisites]({{site.baseurl}}/docs/using-the-jdbc-driver/#prerequisites).
2. [Get the Drill JDBC Driver]({{site.baseurl}}/docs/using-the-jdbc-driver/#getting-the-drill-jdbc-driver).
3. Put the Drill JDBC jar file on the classpath.
4. Use a [valid URL]({{site.baseurl}}/docs/using-the-jdbc-driver/#using-the-jdbc-url-for-a-random-drillbit-connection) in the JDBC connection string when you write application code or configure BI tools.
5. Use the [name of the Drill driver class]({{site.baseurl}}/docs/using-the-jdbc-driver/#using-the-drill-driver-class-name) in application code or in the configuration of client tools.

Most client tools provide a UI where you can enter all of the required connection information, including the driver location, connection URL, and driver class name. For specific examples of client tool connections to Drill through JDBC, see [Using JDBC with SQuirreL]({{ site.baseurl }}/docs/using-jdbc-with-squirrel-on-windows) and [Configuring Spotfire Server]({{ site.baseurl }}/docs/configuring-tibco-spotfire-server-with-drill/).

## Prerequisites

  * JRE 7 or JDK 7  
  * [A Drill installation]({{site.baseurl}}/docs/install-drill/)  
  * Capability to resolve the actual host name of the Drill node(s) with the IP(s), as described in step 4 of [Install the Drill JDBC Driver with JReport]({{site.baseurl}}/docs/configuring-jreport-with-drill/#step-1:-install-the-drill-jdbc-driver-with-jreport). 

## Getting the Drill JDBC Driver

The Drill JDBC Driver `JAR` file must exist on a client machine so you can configure the driver for the application or third-party tool that you intend to use. Obtain the driver in one of the following ways:

* Copy the `drill-jdbc-all` JAR file from the following Drill installation directory on a node where Drill is installed to a directory on your client machine:  
   `<drill_installation_directory>/jars/jdbc-driver/drill-jdbc-all-<version>.jar`  

   Or

* Download the following TAR file to a location on your client machine: [apache-
drill-1.0.0.tar.gz](http://apache.osuosl.org/drill/drill-1.0.0/apache-drill-1.0.0-src.tar.gz). Extract the file. On Windows, you may need to use a decompression utility, such as [7-zip](http://www.7-zip.org/). The driver is extracted to the following directory:  
   `<drill-installation_directory>/jars/jdbc-driver/drill-jdbc-all-<version>.jar`

## Using the JDBC URL for a Random Drillbit Connection 

The format of the JDBC URL differs slightly, depending on the way you want to connect to the Drillbit: random, local, or direct. This section covers using the URL for a random or local connection. Using a URL to [directly connect to a Drillbit]({{site.baseurl}}/docs/using-the-jdbc-driver/#using-the-jdbc-url-format-for-a-direct-drillbit-connection) is covered later. If you want ZooKeeper to randomly choose a Drillbit in the cluster, or if you want to connect to the local Drillbit, the format of the driver URL is:

`jdbc:drill:zk=<zk name>[:<port>][,<zk name2>[:<port>]... `  
  `<directory>/<cluster ID>;[schema=<storage plugin>]`

where

* `jdbc` is the connection type. Required.  
* `schema` is the name of a [storage plugin]({{site.baseurl}}/docs/storage-plugin-registration) configuration to use as the default for queries. For example,`schema=hive`. Optional.  
* `zk name` specifies one or more ZooKeeper host names, or IP addresses. Use `local` instead of a host name or IP address to connect to the local Drillbit. Required. 
* `port` is the ZooKeeper port number. Port 2181 is the default. On a MapR cluster, the default is 5181. Optional. 
* `directory` is the Drill directory in ZooKeeper, which by default is `/drill`. Optional. 
* `cluster ID` is `drillbits1` by default. If the default has changed, [determine the cluster ID]({{site.baseurl}}/docs/using-the-jdbc-driver/#determining-the-cluster-id) and use it. Optional.

### Determining the Cluster ID

To determine the cluster ID, check the following file:

`<drill-installation>/conf/drill-override.conf`

For example:

`...
drill.exec: {
  cluster-id: "docs41cluster-drillbits",
  zk.connect: "centos23.lab:5181,centos28.lab:5181,centos29.lab:5181"
}
...`

### URL Examples

**Single-Node Installation**

`jdbc:drill:zk=maprdemo:5181`

`jdbc:drill:zk=centos23.lab:2181/drill/docs41cluster-drillbits`

`jdbc:drill:zk=10.10.100.56:2181/drill/drillbits1;schema=hive`

**Cluster Installation**

`jdbc:drill:zk=10.10.100.30:5181,10.10.100.31:5181,10.10.100.32:2181/drill/drillbits1;schema=hive`

## Using the JDBC URL Format for a Direct Drillbit Connection

If you want to connect directly to a Drillbit instead of using ZooKeeper to choose the Drillbit, replace `zk=<zk name>` with `drillbit=<node>` as shown in the following URL.

`jdbc:drill:drillbit=<node name>[:<port>][,<node name2>[:<port>]... `  
  `<directory>/<cluster ID>[schema=<storage plugin>]`

where

`drillbit=<node name>` specifies one or more host names or IP addresses of cluster nodes running Drill. 

For definitions of other URL components, see [Using the JDBC URL for a Random Drillbit Connection]({{site.baseurl}}/docs/using-the-jdbc-driver/#using-the-jdbc-url-for-a-random-drillbit-connection).

## Using the Drill Driver Class Name

The class name for the JDBC driver is [org.apache.drill.jdbc.Driver]({{site.baseurl}}/api/1.2/jdbc/). For details, see the Apache Drill JDBC Driver version 1.2.0 [Javadoc]({{site.baseurl}}/api/1.2/jdbc/).

## Example of Connecting to Drill Programmatically

The following sample code shows using the class name in a snippet that connects to Drill using the Drill-Jdbc-all driver:

```
Class.forName("org.apache.drill.jdbc.Driver");
Connection connection =DriverManager.getConnection("jdbc:drill:zk=
node3.mynode.com:2181/drill/my_cluster_com-drillbits");
Statement st = connection.createStatement();
ResultSet rs = st.executeQuery("SELECT * from cp.`employee`");
while(rs.next()){
System.out.println(rs.getString(1));
}
```
