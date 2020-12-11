---
title: "Distributed Mode Prerequisites"
slug: "Distributed Mode Prerequisites"
parent: "Installing Drill in Distributed Mode"
---
You can install Apache Drill on one or more nodes to run it in a clustered environment.

## Prerequisites

Before you install Drill on nodes in a cluster, ensure that the cluster meets the following prerequisites:

  * (Required) Running Oracle or OpenJDK 8        
  * (Required) Running a [ZooKeeper quorum](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html#sc_RunningReplicatedZooKeeper)  
  * (Recommended) Running a Hadoop cluster   
  * (Recommended) Using DNS

{% include startnote.html %}Starting in Drill 1.18 the bundled ZooKeeper libraries are upgraded to version 3.5.7, preventing connections to older (< 3.5) ZooKeeper clusters.  In order to connect to a ZooKeeper < 3.5 cluster, replace the ZooKeeper library JARs in `${DRILL_HOME}/jars/ext` with zookeeper-3.4.x.jar then restart the cluster.{% include endnote.html %}
