---
title: "Install Drill Introduction"
date: 2015-12-28 21:37:19 UTC
parent: "Install Drill"
---

If you installed Drill 1.2 or earlier and generated Parquet files, you need to [migrate the files]({{site.baseurl}}/docs/migrating-parquet-data) for use in later releases as explained in the next section.

You can install Drill in either embedded mode or distributed mode. Installing
Drill in embedded mode does not require any configuration. To use Drill in a
clustered Hadoop environment, install Drill in distributed mode. You need to perform some configuration after installing Drill in distributed mode. After you complete these tasks, connect Drill to your Hive, HBase, or distributed file system data sources, and run queries on them.
