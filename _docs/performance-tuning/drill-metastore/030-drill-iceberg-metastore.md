---
title: "Drill Iceberg Metastore"
parent: "Drill Metastore"
date: 2020-03-03
---

Drill's Metastore allows a variety of storage engines. Drill ships with a storage engine based on
 [Iceberg tables](http://iceberg.incubator.apache.org). For Drill 1.17, this is default Drill Metastore
 implementation.

{% include startnote.html %}
Iceberg table supports concurrent writes and transactions but they are only effective on file systems that support
 atomic rename.
If the file system does not support atomic rename, it could lead to inconsistencies during concurrent writes.
{% include endnote.html %}

### Iceberg Tables Location

Iceberg is essentially a file system within a file. The Iceberg table is stored in a file system. In the tutorial we
 stored the Metastore in your local file system. Drill is a distributed query engine, so production deployments MUST
 store the Metastore on DFS such as HDFS.

Iceberg Metastore configuration can be set in `drill-metastore-distrib.conf` or `drill-metastore-override.conf` files.
The default configuration is indicated in `drill-metastore-module.conf` file.

Within the Metastore directory, the Metastore stores the data for each table as a set of records within a single Iceberg table.

Iceberg tables will reside on the file system in the location based on Iceberg Metastore base location
 `drill.metastore.iceberg.location.base_path` configuration property and component specific location.
Table metadata resides in the `${drill.metastore.iceberg.location.base_path}/tables` directory.

If you ran through the [Tutorial]({{site.baseurl}}/docs/using-drill-metastore/#tutorial), Metastore files are stored in `/home/username/drill/metastore/iceberg/tables`.
If you inspect this directory you will see the following directories `my-dfs/tmp/lineitem/`.

Example of base Metastore configuration file `drill-metastore-override.conf`, where Iceberg tables are stored in
 hdfs:

```
drill.metastore.iceberg: {
  config.properties: {
    fs.defaultFS: "hdfs:///"
  }

  location: {
    base_path: "/drill/metastore",
    relative_path: "iceberg"
  }
}
```

### Iceberg metadata expiration

Iceberg table generates metadata for each modification operation:
snapshot, manifest file, table metadata file. Also when performing delete operation,
previously stored data files are not deleted. These files with the time can occupy lots of space.
Two table properties `write.metadata.delete-after-commit.enabled` and `write.metadata.previous-versions-max`
control expiration process. Metadata files will be expired automatically if
`write.metadata.delete-after-commit.enabled` is enabled.
