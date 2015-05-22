---
title: "Installing Drill on the Cluster"
parent: "Installing Drill in Distributed Mode"
---
Complete the following steps to install Drill on designated nodes:

  1. Download the Drill tarball.
  
        curl http://getdrill.org/drill/download/apache-drill-1.0.0.tar.gz
  2. Explode the tarball to the directory of your choice, such as `/opt`:
  
        tar -xzvf apache-drill-1.0.0.tar.gz
  3. In `drill-override.conf,` create a unique Drill `cluster ID`, and provide Zookeeper host names and port numbers to configure a connection to your Zookeeper quorum.
     1. Edit `drill-override.conf` located in the `/conf` directory.
     2. Provide a unique `cluster-id` and the Zookeeper host names and port numbers in `zk.connect`. If you install Drill on multiple nodes, assign the same `cluster ID` to each Drill node so that all Drill nodes share the same ID. The default Zookeeper port is 2181.

       **Example**
       
         drill.exec:{
          cluster-id: "<mydrillcluster>",
          zk.connect: "<zkhostname1>:<port>,<zkhostname2>:<port>,<zkhostname3>:<port>"
         }

You can connect Drill to various types of data sources. Refer to [Connect Apache Drill to Data Sources]({{ site.baseurl }}/docs/connect-a-data-source-introduction) to get configuration instructions for the
particular type of data source that you want to connect to Drill.