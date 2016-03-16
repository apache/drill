---
title: "Distributed Mode Prerequisites"
date: 2016-03-16 16:09:54 UTC
parent: "Installing Drill in Distributed Mode"
---
You can install Apache Drill on one or more nodes to
run it in a clustered environment.

## Prerequisites

Before you install Drill on nodes in a cluster, ensure that the cluster meets the following prerequisites:

  * (Required) Running Oracle JDK [version 7](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html) or [version 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)  
  As of Drill 1.6, you can build and run Drill with JDK 1.8. If building Drill, note that some unit tests may fail. To avoid this, issue `-DskipTests` to skip the unit tests.      
  * (Required) Running a [ZooKeeper quorum](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html#sc_RunningReplicatedZooKeeper) (Required)  
  * (Recommended) Running a Hadoop cluster   
  * (Recommended) Using DNS 
