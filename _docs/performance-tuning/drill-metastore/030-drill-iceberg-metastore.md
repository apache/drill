---
title: "Drill Iceberg Metastore"
parent: "Drill Metastore"
date:
---

Drill uses Iceberg Metastore implementation based on [Iceberg tables](http://iceberg.incubator.apache.org). For
 details on how to configure Iceberg Metastore implementation and its option descriptions, please refer to
 [Iceberg Metastore docs](https://github.com/apache/drill/blob/master/metastore/iceberg-metastore/README.md).

### Iceberg Tables Location

Iceberg tables will reside on the file system in the location based on
Iceberg Metastore base location `drill.metastore.iceberg.location.base_path` and component specific location.
If Iceberg Metastore base location is `/drill/metastore/iceberg`
and tables component location is `tables`. Iceberg table for tables component
will be located in `/drill/metastore/iceberg/tables` folder.

Metastore metadata will be stored inside Iceberg table location provided
in the configuration file. Drill table metadata location will be constructed
based on specific component storage keys. For example, for `tables` component,
storage keys are storage plugin, workspace and table name: unique table identifier in Drill.

Assume Iceberg table location is `/drill/metastore/iceberg/tables`, metadata for the table
`dfs.tmp.nation` will be stored in the `/drill/metastore/iceberg/tables/dfs/tmp/nation` folder.

Example of base Metastore configuration file `drill-metastore-override.conf`, where Iceberg tables will be stored in
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

### Metadata Storage Format

Iceberg tables support data storage in three formats: Parquet, Avro, ORC. Drill metadata will be stored in Parquet files.
This format was chosen over others since it is column oriented and efficient in terms of disk I/O when specific
columns need to be queried.

Each Parquet file will hold information for one partition. Partition keys will depend on Metastore
component characteristics. For example, for tables component, partitions keys are storage plugin, workspace,
table name and metadata key.

Parquet files name will be based on UUID to ensure uniqueness. If somehow collision occurs, modify operation
in Metastore will fail.

### Iceberg metadata expiration

Iceberg table generates metadata for each modification operation:
snapshot, manifest file, table metadata file. Also when performing delete operation,
previously stored data files are not deleted. These files with the time can occupy lots of space.
Two table properties `write.metadata.delete-after-commit.enabled` and `write.metadata.previous-versions-max`
control expiration process. Metadata files will be expired automatically if
`write.metadata.delete-after-commit.enabled` is enabled.
