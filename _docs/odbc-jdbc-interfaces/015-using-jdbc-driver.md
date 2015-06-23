---
title: "Using the JDBC Driver"
parent: "ODBC/JDBC Interfaces"
---
This section explains how to install and use the JDBC driver for Apache Drill. For specific examples of client tool connections to Drill via JDBC, see [Using JDBC with SQuirreL]({{ site.baseurl }}/docs/using-jdbc-with-squirrel-on-windows) and [Configuring Spotfire Server]({{ site.baseurl }}/docs/configuring-tibco-spotfire-server-with-drill/).


### Prerequisites

  * JRE 7 or JDK 7
  * Drill installed either in embedded mode or in distributed mode on one or more nodes in a cluster. Refer to the [Install Drill]({{ site.baseurl }}/docs/install-drill/) documentation for more information.
  * The client must be able to resolve the actual hostname of the Drill node(s) with the IP(s). Verify that a DNS entry was created on the client machine for the Drill node(s).  If a DNS entry does not exist, create the entry for the Drill node(s).

    * For Windows, create the entry in the %WINDIR%\system32\drivers\etc\hosts file.
    * For Linux and Mac OSX, create the entry in /etc/hosts.  
<drill-machine-IP> <drill-machine-hostname>
    For example: `127.0.1.1 maprdemo`


----------

### Getting the Drill JDBC Driver

The Drill JDBC Driver `JAR` file must exist in a directory on a client machine so you can configure the driver for the application or third-party tool that you intend to use. You can obtain the driver in two different ways:

1. Copy the `drill-jdbc-all` JAR file from the following Drill installation directory on a node where Drill is installed to a directory on your client machine:

        <drill_installation_directory>/jars/jdbc-driver/drill-jdbc-all-<version>.jar
    
    For example, on a MapR cluster: `/opt/mapr/drill/drill-1.0.0/jars/jdbc-driver/drill-jdbc-all-1.0.0-mapr-r1.jar`

2. Download the following tar file to a location on your client machine: [apache-
drill-1.0.0.tar.gz](http://apache.osuosl.org/drill/drill-1.0.0/apache-drill-1.0.0-src.tar.gz) and extract the file. You may need to use a decompression utility, such as [7-zip](http://www.7-zip.org/). The driver is extracted to the following directory:

    <drill-home>\apache-drill-\<version>\jars\jdbc-driver\drill-jdbc-all-<version>.jar

----------

### Configuring a Driver Application or Client

To configure a JDBC application, users have to:

1. Put the Drill JDBC jar file on the class path.
2. Use a valid Drill JDBC URL.
3. Configure tools or application code with the name of the Drill driver class.

Most client tools provide a UI where you can enter all of the required connection information, including the Driver location, connection URL, and driver class name.

### JDBC Driver URLs

The driver URLs that you use to create JDBC connection strings must be formed as follows:

`jdbc:drill:zk=<zookeeper_quorum>:<port>/<drill_directory_in_zookeeper>/<cluster_ID>;schema=<schema_to_use_as_default>`

Any Drill JDBC URL must start with: `jdbc:drill`.

**ZooKeeper Quorum**

To connect to a cluster, specify the ZooKeeper quorum as a list of hostnames or IP addresses. 

**ZooKeeper Port Number**

The default ZooKeeper port is 2181. On a MapR cluster, the ZooKeeper port is 5181.

**Drill Directory in ZooKeeper**

The name of the Drill directory stored in ZooKeeper is `/drill`.

**Cluster ID**

The Drill default cluster ID is <code>drillbits1</code>.

On a MapR cluster, check the following file for the cluster ID:

`/opt/mapr/drill/drill-1.0.0/conf/drill-override.conf`

For example:

`...
drill.exec: {
  cluster-id: "docs41cluster-drillbits",
  zk.connect: "centos23.lab:5181,centos28.lab:5181,centos29.lab:5181"
}
...`

**Schema**

Optionally, include the default schema for the JDBC connection. For example:

`schema=hive`


### URL Examples

**Single-Node Installation**

`jdbc:drill:zk=maprdemo:5181`

`jdbc:drill:zk=centos23.lab:5181/drill/docs41cluster-drillbits`

`jdbc:drill:zk=10.10.100.56:5181/drill/drillbits1;schema=hive`

**Cluster Installation**

`jdbc:drill:zk=10.10.100.30:5181,10.10.100.31:5181,10.10.100.32:5181/drill/drillbits1;schema=hive`

---------

### Driver Class Name

The class name for the JDBC driver is `org.apache.drill.jdbc.Driver`

-----------