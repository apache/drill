---
title: "Installing Drill in Distributed Mode"
parent: "Install Drill"
---
[Previous](/docs/installing-drill-on-windows)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Back to Table of Contents](/docs)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Next](/docs/connect-to-a-data-source)

You can install Apache Drill in distributed mode on one or multiple nodes to
run it in a clustered environment.

To install Apache Drill in distributed mode, complete the following steps:

  1. Install Drill on each designated node in the cluster.
  2. Configure a cluster ID and add Zookeeper information.
  3. Connect Drill to your data sources. 
  4. Start Drill.

**Prerequisites**

Before you install Apache Drill on nodes in your cluster, you must have the
following software and services installed:

  * [Oracle JDK version 7](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)
  * Configured and running ZooKeeper quorum
  * Configured and running Hadoop cluster (Recommended)
  * DNS (Recommended)

## Installing Drill

Complete the following steps to install Drill on designated nodes:

  1. Download the Drill tarball.
  
        curl http://www.apache.org/dyn/closer.cgi/drill/drill-0.7.0/apache-drill-0.7.0.tar.gz
  2. Issue the following command to create a Drill installation directory and then explode the tarball to the directory:
  
        mkdir /opt/drill
        tar xzf apache-drill-<version>.tar.gz --strip=1 -C /opt/drill
  3. If you are using external JAR files, edit `drill-env.sh, `located in `/opt/drill/conf/`, and define `HADOOP_HOME:`
  
        export HADOOP_HOME="~/hadoop/hadoop-0.20.2/"
  4. In `drill-override.conf,` create a unique Drill `cluster ID`, and provide Zookeeper host names and port numbers to configure a connection to your Zookeeper quorum.
     1. Edit `drill-override.conf `located in `~/drill/drill-<version>/conf/`.
     2. Provide a unique `cluster-id` and the Zookeeper host names and port numbers in `zk.connect`. If you install Drill on multiple nodes, assign the same `cluster ID` to each Drill node so that all Drill nodes share the same ID. The default Zookeeper port is 2181.

       **Example**
       
         drill.exec:{
          cluster-id: "<mydrillcluster>",
          zk.connect: "<zkhostname1>:<port>,<zkhostname2>:<port>,<zkhostname3>:<port>",
          debug.error_on_leak: false,
          buffer.size: 6,
         functions: ["org.apache.drill.expr.fn.impl", "org.apache.drill.udfs"]
         }

You can connect Drill to various types of data sources. Refer to [Connect
Apache Drill to Data Sources](/docs/connect-to-data-sources) to get configuration instructions for the
particular type of data source that you want to connect to Drill.