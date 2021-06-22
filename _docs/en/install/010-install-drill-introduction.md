---
title: "Install Drill Introduction"
slug: "Install Drill Introduction"
parent: "Install Drill"
---

You can install Drill for use in either embedded mode or distributed mode. [Choose embedded mode]({{site.baseurl}}/docs/installing-drill-in-embedded-mode/) to use Drill only on a single node. Installing Drill for use in embedded mode does not require installation of ZooKeeper. Using Drill in embedded mode requires no configuration.

[Choose distributed mode]({{site.baseurl}}/docs/installing-drill-in-distributed-mode/) to use Drill in a clustered Hadoop environment. A clustered (multi-server) installation of ZooKeeper is one of the [prerequisites]({{site.baseurl}}/docs/distributed-mode-prerequisites/). You also need to configure Drill for use in distributed mode. After you complete these tasks, connect Drill to your Hive, HBase, or distributed file system data sources, and run queries on them.

## Using Parquet Files from a Previous Installation
If you installed Drill 1.2 or earlier and generated Parquet files, you need to [migrate the files]({{site.baseurl}}/docs/migrating-parquet-data) for use in later releases as explained in the next section.
