---
title: "Distributed Mode Prerequisites"
slug: "Distributed Mode Prerequisites"
parent: "Installing Drill in Distributed Mode"
---
You can install Apache Drill on one or more nodes to
run it in a clustered environment.

## Prerequisites

Before you install Drill on nodes in a cluster, ensure that the cluster meets the following prerequisites:

  * (Required) Running Oracle or OpenJDK 8        
  * (Required) Running a [ZooKeeper quorum](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html#sc_RunningReplicatedZooKeeper)  
  * (Recommended) Running a Hadoop cluster   
  * (Recommended) Using DNS

    {% include startnote.html %}Start from Drill 1.18, ZK version is upgraded to 3.5.7. So by default, we can't connect to the old ZK cluster (< 3.5). If we need to use the previous ZK, let's do this: first, delete the zookeeper dependency files in `${DRILL_HOME}/jars/ext`, then copy zookeeper-3.4.x.jar to the directory, and finally restart the cluster.{% include endnote.html %}
