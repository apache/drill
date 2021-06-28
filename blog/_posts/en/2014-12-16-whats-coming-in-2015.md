---
layout: post
title: "What's Coming in 2015?"
code: whats-coming-in-2015
excerpt: Drill is now a top-level project, and the community is expanding rapidly. Find out more about some of the new features planned for 2015.
authors: ["tshiran"]
---
2014 was an exciting year for the Drill community. In August we made Drill available for downloads, and last week the Apache Software Foundation promoted Drill to a top-level project. Many of you have asked me what's coming next, so I decided to sit down and outline some of the interesting initiatives that the Drill community is currently working on:

* Flexible Access Control
* JSON in Any Shape or Form
* Advanced SQL
* New Data Sources
* Drill/Spark Integration
* Operational Enhancements: Speed, Scalability and Workload Management

This is by no means intended to be an exhaustive list of everything that will be added to Drill in 2015. With Drill's rapidly expanding community, I anticipate that you'll see a whole lot more.

## Flexible Access Control

Many organizations are now interested in providing Drill as a service to their users, supporting many users, groups and organizations with a single cluster. To do so, they need to be able to control who can access what data. Today's volume and variety of data requires a new approach to access control. For example, it is becoming impractical for organizations to manage a standalone, centralized repository of permissions for every column/row of every table. Drill's virtual datasets (views) provide a more scalable solution to access control:

* The user creates a virtual dataset (`CREATE VIEW vd AS SELECT ...`), selecting the data to be exposed/shared. The virtual dataset is defined as a SQL statement. For example, a virtual dataset may represent only the records that were created in the last 30 days and don't have the `restricted` flag. It could even mask some columns. Drill's virtual datasets (just the SQL statement) are stored as files in the file system, so users can leverage file system permissions to control who can access the virtual dataset, without granting access to the source data.
* A virtual dataset is owned by a specific user and can only "select" data that the owner has access to. The data sources (HDFS, HBase, MongoDB, etc.) are responsible for access control decisions. Users and administrators do not need to define separate permissions inside Drill or utilize yet another centralized permission repository, such as Sentry and Ranger.

## JSON in Any Shape or Form

When data is **Big** (as in Big Data), it is painful to copy and transform it. Users should be able to explore the raw data without (or at least prior to) transforming it into another format. Drill is designed to enable in-situ analytics. Just point it at a file or directory and run the queries.

JSON has emerged as the most common self-describing format, and Drill is able to query JSON files out of the box. Drill currently assumes that the JSON documents (or records) are stored sequentially in a file:

```json
{ "name": "Lee", "yelping_since": "2012-02" }
{ "name": "Matthew", "yelping_since": "2011-12" }
{ "name": "Jasmine", "yelping_since": "2010-09" }
```

However, many JSON-based datasets, ranging from [data.gov](http://data.gov) (government) datasets to Twitter API responses, are not organized as simple sequences of JSON documents. In some cases the actual records are listed as elements of an internal array inside a single JSON document. For example, consider the following file, which technically consists of a single JSON document, but really contains three records (under the `data.records` field):

```json
{
  "metadata": ...,
  "data": {
    "records": [
      { "name": "Lee", "yelping_since": "2012-02" },
      { "name": "Matthew", "yelping_since": "2011-12" },
      { "name": "Jasmine", "yelping_since": "2010-09" }
    ]
  }
}
```

The `FLATTEN` function in Drill 0.7+ takes an array and converts each item into a top-level record:

```sql
SELECT FLATTEN(data.records) FROM dfs.tmp.`foo.json`;
```

You can use this as an inner query (or inside a view):

```sql
> SELECT t.record.name AS name
  FROM (SELECT FLATTEN(data.records) AS record FROM dfs.tmp.`test/foo.json`) t;
+------------+
|    name    |
+------------+
| Lee        |
| Matthew    |
| Jasmine    |
+------------+
```

While this works today, the dataset is technically a single JSON document, so Drill ends up reading the entire dataset into memory. We're developing a FLATTEN-pushdown mechanism that will enable the JSON reader to emit the individual records into the downstream operators, thereby making this work with datasets of arbitrary size. Once that's implemented, users will be able to explore any JSON-based dataset in-situ (ie, without having to transform it).

## Full SQL

Unlike the majority of SQL engines for Hadoop and NoSQL databases, which support SQL-like languages (HiveQL, CQL, etc.), Drill is designed from the ground up to be compliant with ANSI SQL. We simply started with a real SQL parser (Apache Calcite, previously known as Optiq). We're currently implementing the remaining SQL constructs, and plan to support the full TPC-DS suite (with no query modifications) in 2015. Full SQL support makes BI tools work better, and enables users who are proficient with SQL to leverage their existing knowledge and skills.

## New Data Sources

Drill is a standalone, distributed SQL engine. It has a pluggable architecture that allows it to support multiple data sources. Drill 0.6 includes storage plugins for:

* [Hadoop File System](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html) implementations (local file system, HDFS, MapR-FS, Amazon S3, etc.)
* HBase and MapR-DB
* MongoDB
* Hive Metastore (query any dataset that is registered in Hive Metastore)

A single query can join data from different systems. For example, a query can join user profiles in MongoDB with log files in Hadoop, or datasets in multiple Hadoop clusters.

I'm eager to see what storage plugins the community develops over the next 12 months. In the last few weeks alone, developers in the community have expressed their desire (on the [public list](mailto:dev@drill.apache.org)) to develop additional storage plugins for the following data sources:

* Cassandra
* Solr
* JDBC (any RDBMS, including Oracle, MySQL, PostgreSQL and SQL Server)

If you're interested in implementing a new storage plugin, I would encourage you to reach out to the Drill developer community on <dev@drill.apache.org>. I'm looking forward to publishing an example of a single-query join across 10 data sources.

## Drill/Spark Integration

We're seeing growing interest in Spark as an execution engine for data pipelines, providing an alternative to MapReduce. The Drill community is working on integrating Drill and Spark to address a few new use cases:

* Use a Drill query (or view) as the input to Spark. Drill is a powerful engine for extracting and pre-processing data from various data sources, thereby reducing development time and effort. Here's an example:

    ```scala
    val sc = new SparkContext(conf)
    val result = sc.drillRDD("SELECT * FROM dfs.root.`path/to/logs` l, mongo.mydb.users u WHERE l.user_id = u.id GROUP BY ...")
    val formatted = result.map { r =>
      val (first, last, visits) = (r.name.first, r.name.last, r.visits)
      s"$first $last $visits"
    }
    ```
  
* Use Drill to query Spark RDDs. Analysts will be able to use BI tools like MicroStrategy, Spotfire and Tableau to query in-memory data in Spark. In addition, Spark developers will be able to embed Drill execution in a Spark data pipeline, thereby enjoying the power of Drill's schema-free, columnar execution engine.

## Operational Enhancements

As we continue with our monthly releases and march towards the 1.0 release early next year, we're focused on improving Drill's speed and scalability. We'll also enhance Drill's multi-tenancy with more advanced workload management.

* **Speed**: Drill is already extremely fast, and we're going to make it even faster over the next few months. With that said, we think that improving user productivity and time-to-insight is as important as shaving a few milliseconds off a query's runtime.
* **Scalability**: To date we've focused mainly on clusters of up to a couple hundred nodes. We're currently working to support clusters with thousands of nodes. We're also improving concurrency to better support deployments in which hundreds of analysts or developers are running queries at the same time.
* **Workload management**: A single cluster is often shared among many users and groups, and everyone expects answers in real-time. Workload management prioritizes the allocation of resources to ensure that the most important workloads get done first so that business demands can be met. Administrators need to be able to assign priorities and quotas at a fine granularity. We're working on enhancing Drill's workload management to provide these capabilities while providing tight integration with YARN and Mesos.

## We Would Love to Hear From You!

Are there other features you would like to see in Drill? We would love to hear from you:

* Drill users: <user@drill.apache.org>
* Drill developers: <dev@drill.apache.org>
* Me: <tshiran@apache.org>

Happy Drilling!  
Tomer Shiran