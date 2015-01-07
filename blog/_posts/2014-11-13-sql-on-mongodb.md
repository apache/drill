---
layout: post
title: "SQL on MongoDB"
code: sql-on-mongodb
excerpt: The MongoDB storage plugin for Drill enables analytical queries on MongoDB databases. Drill's schema-free JSON data model is a natural fit for MongoDB's data model.
date: 2014-11-19 13:50:01
authors: ["akumar", "kbhallamudi"]
---
One of the many interesting and unique features about Drill is its ability to support multiple query languages, data formats, and data sources, as well as cross-platform querying (such as joining HBase tables with your Mongo collections) using ANSI SQL.

As of now, Drill supports multiple storage plugins, including HDFS, HBase, Hive, and LocalFileSystem. Since Drill is optimized for nested data, we realized that a Mongo storage plugin would be a useful feature.

So, recently Kamesh and I (we have an avid interest in all things Mongo) contributed the MongoDB storage plugin feature to the Apache Drill project. As part of this blog post, we would like to provide instructions on how to use this plugin, which has been included in the [Drill 0.6 release](http://incubator.apache.org/drill/download/).

The instructions are divided into the following subtopics:

* Drill and Mongo setup (standalone/replicated/sharded)
* Running queries
* Securely accessing MongoDB
* Optimizations

## Drill and MongoDB Setup (Standalone/Replicated/Sharded)

### Standalone
* Start `mongod` process ([Install MongoDB](http://docs.mongodb.org/manual/installation/))
* Start Drill in embedded mode ([Installing Drill in Embedded Mode](https://cwiki.apache.org/confluence/display/DRILL/Installing+Drill+in+Embedded+Mode) & [Starting/Stopping Drill](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=44994063)) 
* Access the Web UI through the local drillbit: <http://localhost:8047/>
* Enable the Mongo storage plugin and update its configuration:

    ```json
    {
      "type": "mongo",
      "connection": "mongodb://localhost:27017",
      "enabled": true
    }
    ```

  By default, `mongod` listens on port 27017.

![Drill on MongoDB in standalone mode]({{ site.baseurl }}/static/{{ page.code }}/standalone.png)

### Replica Set
* Start `mongod` processes in replication mode
* Start Drill in distributed mode ([Installing Drill in Distributed Mode](https://cwiki.apache.org/confluence/display/DRILL/Installing+Drill+in+Distributed+Mode) & [Starting/Stopping Drill](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=44994063))
* Access the Web UI through any drillbit: <http://drillbit2:8047>
* Enable the Mongo storage plugin and update its configuration:

    ```json
    {
      "type": "mongo",
      "connection": "mongodb://<host1>:<port1>,<host2>:<port2>",
      "enabled": true
    }
    ```

    Where `host1` and `host2` are `mongod` hostnames in the replica set.

![Drill on MongoDB in replicated mode]({{ site.baseurl }}/static/{{ page.code }}/replicated.png)

In replicated mode, whichever drillbit receives the query connects to the nearest `mongod` (local `mongod`) to read the data.

### Sharded/Sharded with Replica Set

* Start Mongo processes in sharded mode
* Start Drill in distributed mode ([Installing Drill in Distributed Mode](https://cwiki.apache.org/confluence/display/DRILL/Installing+Drill+in+Distributed+Mode) & [Starting/Stopping Drill](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=44994063))
* Access the Web UI through any drillbit: <http://drillbit3:8047>
* Enable the Mongo storage plugin and update its configuration:

    ```json
    { 
      "type": "mongo",
      "connection": "mongodb://<host1>:<port1>,<host2>:<port2>",
      "enabled": true
    }
    ```

    Where `host1` and `host2` are the `mongos` hostnames.

![Drill on MongoDB in sharded mode]({{ site.baseurl }}/static/{{ page.code }}/sharded.png)
 
In sharded mode, drillbit first connects to the `mongos` server to get the shard information.

## Endpoint Assignments

Drill is designed to maximize data locality:

* When drillbits and shards run together on the same machines, each drillbit (endpoint) will read the chunks from the local shard. That is, all the chunks from a shard will be assigned to its local drillbit. This is known as data locality, and is the ideal scenario.
* When all drillbits and shards are running on different machines, chunks will be assigned to drillbits in a round-robin fashion. In this case there is no data locality.
* When some of drillbits and shards are colocated, and some of them are running on different machines, partial data locality is achieved.

## Running Queries

Here is a simple exercise that provides steps for creating an `empinfo` collection in an `employee` database in Mongo that you can query using Drill:

1. Download [zips.json](http://media.mongodb.org/zips.json) and the [empinfo.json]({{ site.baseurl }}/static/{{ page.code }}/empinfo.json) dataset referenced at the end of blog.
2. Import the zips.json and empinfo.json files into Mongo using the following command:  

    ```bash
    mongoimport --host localhost --db test --collection zips < zips.json
    mongoimport --host localhost --db employee --collection empinfo < empinfo.json
    ```

3. Issue the following queries either from sqlline (Drill’s shell) or from the Drill Web UI to get corresponding results from the Mongo collection. 
   * To issue queries from the web UI, open the Drill web UI and go to Query tab. 
   * To issue queries from sqlline, connect to sqlline using the following command: 

        ```bash
        <DRILLHOME>/bin/sqlline -u jdbc:drill:zk=zkhost:2181 -n admin -p admin
        ```

4. Queries:

    ```sql
    SELECT first_name, last_name, position_id
    FROM mongo.employee.`empinfo`
    WHERE employee_id = 1107 AND position_id = 17 AND last_name = 'Yonce';  
    
    SELECT city, sum(pop)
    FROM mongo.test.`zips` zipcodes
    WHERE state IS NOT NULL GROUP BY city
    ORDER BY sum(pop) DESC LIMIT 1;
    ```

*Note*: If a field contains a mixture of different data types across different records, such as both int and decimal values, then queries fail unless `store.mongo.all_text_mode = true` and aggregations fail in that case. For more information refer to [DRILL-1475](https://issues.apache.org/jira/browse/DRILL-1475) and [DRILL-1460](https://issues.apache.org/jira/browse/DRILL-1460).

To set `store.mongo.all_text_mode = true`, execute the following command in sqlline:

```sql
alter session set store.mongo.all_text_mode = true
```

## Securely Accessing MongoDB
Create two databases, emp and zips. For each database, create a user with read privileges. As an example, for the zips database, create a user “apache” with read privileges. For the emp database, create a user “drill” with read privileges.

The apache user will be able to query the zips database by using the following storage plugin configuration:

```json
{ 
  "type": "mongo",
  "connection": "mongodb://apache:apache@localhost:27017/zips",
  "enabled": true
}
```

The `drill` user will be able to query the `emp` database by using the following storage plugin configuration:

```json
{ 
  "type": "mongo",
  "connection": "mongodb://drill:drill@localhost:27017/emp",
  "enabled": true 
}
```

*Note*: The security patch may be included in next release. Check [DRILL-1502](https://issues.apache.org/jira/browse/DRILL-1502) for status.

## Optimizations
The MongoDB storage plugin supports predicate pushdown and projection pushdown. As of now, predicate pushdown is implemented for the following filters: `>`, `>=`, `<`, `<=`, `==`, `!=`, `isNull` and `isNotNull`.

We are excited about the release of the MongoDB storage plugin, and we believe that Drill is the perfect SQL query tool for MongoDB.