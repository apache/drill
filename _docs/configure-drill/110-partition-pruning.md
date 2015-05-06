---
title: "Partition Pruning"
parent: "Configure Drill"
---
Partition pruning is a performance optimization that limits the number of
files and partitions that Drill reads when querying file systems and Hive
tables. Drill only reads a subset of the files that reside in a file system or
a subset of the partitions in a Hive table when a query matches certain filter
criteria.

For Drill to apply partition pruning to Hive tables, you must have created the
tables in Hive using the `PARTITION BY` clause:

`CREATE TABLE <table_name> (<column_name>) PARTITION BY (<column_name>);`

When you create Hive tables using the `PARTITION BY` clause, each partition of
data is automatically split out into different directories as data is written
to disk. For more information about Hive partitioning, refer to the [Apache
Hive wiki](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL/#LanguageManualDDL-PartitionedTables).

Typically, table data in a file system is organized by directories and
subdirectories. Queries on table data may contain `WHERE` clause filters on
specific directories.

Drill’s query planner evaluates the filters as part of a Filter operator. If
no partition filters are present, the underlying Scan operator reads all files
in all directories and then sends the data to operators downstream, such as
Filter.

When partition filters are present, the query planner determines if it can
push the filters down to the Scan such that the Scan only reads the
directories that match the partition filters, thus reducing disk I/O.

## Partition Pruning Example

The /`Users/max/data/logs` directory in a file system contains subdirectories
that span a few years.

The following image shows the hierarchical structure of the `…/logs` directory
and (sub) directories:

![drill query flow]({{ site.baseurl }}/docs/img/54.png)

The following query requests log file data for 2013 from the `…/logs`
directory in the file system:

    SELECT * FROM dfs.`/Users/max/data/logs` WHERE cust_id < 10 and dir0 = 2013 limit 2;

If you run the `EXPLAIN PLAN` command for the query, you can see that the`
…/logs` directory is filtered by the scan operator.

    EXPLAIN PLAN FOR SELECT * FROM dfs.`/Users/max/data/logs` WHERE cust_id < 10 and dir0 = 2013 limit 2;

The following image shows a portion of the physical plan when partition
pruning is applied:

![drill query flow]({{ site.baseurl }}/docs/img/21.png)

## Filter Examples

The following queries include examples of the types of filters eligible for
partition pruning optimization:

**Example 1: Partition filters ANDed together**

    SELECT * FROM dfs.`/Users/max/data/logs` WHERE dir0 = '2014' AND dir1 = '1'

**Example 2: Partition filter ANDed with regular column filter**

    SELECT * FROM dfs.`/Users/max/data/logs` WHERE cust_id < 10 AND dir0 = 2013 limit 2;

**Example 3: Combination of AND, OR involving partition filters**

    SELECT * FROM dfs.`/Users/max/data/logs` WHERE (dir0 = '2013' AND dir1 = '1') OR (dir0 = '2014' AND dir1 = '2')

