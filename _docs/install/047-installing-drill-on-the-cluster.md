---
title: "Installing Drill on the Cluster"
date: 2018-12-30
parent: "Installing Drill in Distributed Mode"
---
You install Drill on nodes in the cluster, configure a cluster ID, and add Zookeeper information, as described in the following steps:

  1. Download the latest version of Apache Drill [here](http://apache.mirrors.hoobly.com/drill/drill-1.15.0/apache-drill-1.15.0.tar.gz) or from the [Apache Drill mirror site](http://www.apache.org/dyn/closer.cgi/drill/drill-1.15.0/apache-drill-1.15.0.tar.gz) with the command appropriate for your system:  
       * `wget http://apache.mirrors.hoobly.com/drill/drill-1.15.0/apache-drill-1.15.0.tar.gz`  
       * `curl -o apache-drill-1.15.0.tar.gz http://apache.mirrors.hoobly.com/drill/drill-1.15.0/apache-drill-1.15.0.tar.gz`  
  2. Extract the tarball to the directory of your choice, such as `/opt`:  
  `tar -xzvf apache-drill-<version>.tar.gz`
  3. In `drill-override.conf,` use the Drill `cluster ID`, and provide ZooKeeper host names and port numbers to configure a connection to your ZooKeeper quorum.  
         a. Edit `drill-override.conf` located in the `conf` directory.  
         b. Provide a unique `cluster-id` and the ZooKeeper host names and port numbers in `zk.connect`. If you install Drill on multiple nodes, assign the same `cluster ID` to each Drill node so that all Drill nodes share the same ID. The default ZooKeeper port on the open source version of Apache Drill is 2181.

       **Example**
       
         drill.exec:{
          cluster-id: "<mydrillcluster>",
          zk.connect: "<zkhostname1>:<port>,<zkhostname2>:<port>,<zkhostname3>:<port>"
         }

