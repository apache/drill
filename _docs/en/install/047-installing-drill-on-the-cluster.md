---
title: "Installing Drill on the Cluster"
slug: "Installing Drill on the Cluster"
parent: "Installing Drill in Distributed Mode"
---

You install Drill on nodes in the cluster, configure a cluster ID, and add Zookeeper information, as described in the following steps:

### Install

1. Download the latest version of Apache Drill [here](http://apache.mirrors.hoobly.com/drill/drill-1.19.0/apache-drill-1.19.0.tar.gz) or from the [Apache Drill mirror site](http://www.apache.org/dyn/closer.cgi/drill/drill-1.19.0/apache-drill-1.19.0.tar.gz) with the command appropriate for your system:
   - `wget http://apache.mirrors.hoobly.com/drill/drill-1.19.0/apache-drill-1.19.0.tar.gz`
   - `curl -o apache-drill-1.19.0.tar.gz http://apache.mirrors.hoobly.com/drill/drill-1.19.0/apache-drill-1.19.0.tar.gz`
2. Extract the tarball to the directory of your choice, such as `/opt`:
   `tar -xf apache-drill-<version>.tar.gz`

### (Optional) Create the site directory

The site directory contains your site-specific files for Drill.  Putting these in a separate directory to the Drill installation means that upgrading Drill will not clobber your configuration and custom code.  It is possible to skip this step, meaning that your configuration and custom code will live in the `$DRILL_HOME/conf` and `$DRILL_HOME/jars/3rdparty` subdirectories respectively.

Create the site directory in a suitable location, e.g.

```sh
export DRILL_SITE=/etc/drill-site
mkdir $DRILL_SITE
```

When you do a fresh install, Drill includes a conf directory under $DRILL_HOME. Use the files
in that directory to create your site directory.

```sh
cp $DRILL_HOME/conf/drilloverrideexample.conf \
$DRILL_SITE/drilloverride.conf
cp $DRILL_HOME/conf/drill-on-yarnexample.conf \
$DRILL_SITE/drill-on-yarn.conf
cp $DRILL_HOME/conf/drillenv.sh $DRILL_SITE
```

You will use the site directory each time you start Drill by using the `--site` (or `--config`) option. The following are examples.

```sh
drillbit.sh --site $DRILL_SITE
drill-on-yarn.sh --site $DRILL_SITE
```

Once you have created your site directory, upgrades are trivial. Simply delete the old Drill
distribution and install the new one. Your files remain unchanged in the site directory.

### ZooKeeper configuration

In `drill-override.conf,` use the Drill `cluster ID`, and provide ZooKeeper host names and port numbers to configure a connection to your ZooKeeper quorum.

1. Edit `drill-override.conf` located in the `conf` directory.
2. Provide a unique `cluster-id` and the ZooKeeper host names and port numbers in `zk.connect`. If you install Drill on multiple nodes, assign the same cluster ID to each Drill node so that all Drill nodes share the same ID. The default ZooKeeper port on the open source version of Apache Drill is 2181.

**Example**

```hocon
drill.exec:{
  cluster-id: "<mydrillcluster>",
  zk.connect: "<zkhostname1>:<port>,<zkhostname2>:<port>,<zkhostname3>:<port>"
}
```

### Custom JARs

If you develop custom code (data sources or user-defined functions, a.k.a. UDFs), place the Java JAR
files in $DRILL_SITE/jars.

