---
title: "Installing Drill on the Cluster"
date:  
parent: "Installing Drill in Distributed Mode"
---
You install Drill on nodes in the cluster, and then configure a cluster ID and add Zookeeper information, as described in the following steps:

  1. Download the Drill tarball. For example, enter the curl command using the latest version number for Drill:
  
      `curl http://getdrill.org/drill/download/apache-drill-<version>.tar.gz`
  2. Extract the tarball to the directory of your choice, such as `/opt`:
  
      `tar -xzvf apache-drill-<version>.tar.gz`
  3. In `drill-override.conf,` use the Drill `cluster ID`, and provide Zookeeper host names and port numbers to configure a connection to your Zookeeper quorum.
     1. Edit `drill-override.conf` located in the `conf` directory.
     2. Provide a unique `cluster-id` and the Zookeeper host names and port numbers in `zk.connect`. If you install Drill on multiple nodes, assign the same `cluster ID` to each Drill node so that all Drill nodes share the same ID. The default Zookeeper port on the open source version of Apache Drill is 2181.

       **Example**
       
         drill.exec:{
          cluster-id: "<mydrillcluster>",
          zk.connect: "<zkhostname1>:<port>,<zkhostname2>:<port>,<zkhostname3>:<port>"
         }

