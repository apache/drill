---
title: "Querying Avro Files"
date: 2019-04-16
parent: "Querying a File System"
---

Drill supports files in the [Avro](https://avro.apache.org/) format.
Starting from Drill 1.18, the Avro format supports the [Schema provisioning]({{site.baseurl}}/docs/create-or-replace-schema/#usage-notes) feature.

#### Preparing example data

To follow along with this example, download [sample data file](https://github.com/apache/drill/blob/master/exec/java-exec/src/test/resources/avro/map_string_to_long.avro)
 to your `/tmp` directory.

#### Selecting data from Avro files

We can query all data from the `map_string_to_long.avro` file:

```
select * from dfs.tmp.`map_string_to_long.avro`
```

The query returns the following results:

```
+-----------------+
|      mapf       |
+-----------------+
| {"ki":1,"ka":2} |
+-----------------+
```