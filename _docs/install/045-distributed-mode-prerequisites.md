---
title: "Distributed Mode Prerequisites"
parent: "Installing Drill in Distributed Mode"
---
You can install Apache Drill on one or more nodes to
run it in a clustered environment.

## Prerequisites

Before you install Apache Drill on nodes in your cluster, install and configure the
following software and services:

  * [Oracle JDK version 7](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html) (Required)
  * Configured and running a ZooKeeper quorum (Required)
  * Configured and running a Hadoop cluster (Recommended)
  * DNS (Recommended)

To install Apache Drill in distributed mode, complete the following steps:

  1. Install Drill on nodes in the cluster.
  2. Configure a cluster ID and add Zookeeper information.
  3. Connect Drill to your data sources.

## Connecting Drill to Distributed Data Sources

In a Drill cluster, you typically do not query the local file system, but instead query files on the distributed file system, databases supported through a storage plugin, or the Hive metastore. You use a [storage plugin]({{site.baseurl}}/docs/connect-a-data-source) to connect to these data sources.
