---
title: "Why Drill"
date: 2018-11-02
parent: "Getting Started"
---

## Top 10 Reasons to Use Drill

## 1. Get started in minutes

It takes just a few minutes to get started with Drill. Untar the Drill software on your Linux, Mac, or Windows laptop and run a query on a local file. No need to set up any infrastructure or to define schemas. Just point to the data, such as data in a file, directory, HBase table, and drill.

    $ tar -xvf apache-drill-<version>.tar.gz
    $ <install directory>/bin/drill-embedded
    0: jdbc:drill:zk=local> SELECT * FROM cp.`employee.json` LIMIT 5;
    +--------------+----------------------------+---------------------+---------------+--------------+----------------------------+-----------+----------------+-------------+------------------------+----------+----------------+----------------------+-----------------+---------+-----------------------+
    | employee_id  |         full_name          |     first_name      |   last_name   | position_id  |       position_title       | store_id  | department_id  | birth_date  |       hire_date        |  salary  | supervisor_id  |   education_level    | marital_status  | gender  |    management_role    |
    +--------------+----------------------------+---------------------+---------------+--------------+----------------------------+-----------+----------------+-------------+------------------------+----------+----------------+----------------------+-----------------+---------+-----------------------+
    | 1            | Sheri Nowmer               | Sheri               | Nowmer        | 1            | President                  | 0         | 1              | 1961-08-26  | 1994-12-01 00:00:00.0  | 80000.0  | 0              | Graduate Degree      | S               | F       | Senior Management     |
    | 2            | Derrick Whelply            | Derrick             | Whelply       | 2            | VP Country Manager         | 0         | 1              | 1915-07-03  | 1994-12-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree      | M               | M       | Senior Management     |
    | 4            | Michael Spence             | Michael             | Spence        | 2            | VP Country Manager         | 0         | 1              | 1969-06-20  | 1998-01-01 00:00:00.0  | 40000.0  | 1              | Graduate Degree      | S               | M       | Senior Management     |
    | 5            | Maya Gutierrez             | Maya                | Gutierrez     | 2            | VP Country Manager         | 0         | 1              | 1951-05-10  | 1998-01-01 00:00:00.0  | 35000.0  | 1              | Bachelors Degree     | M               | F       | Senior Management     |


## 2. Schema-free JSON model
Drill is the world's first and only distributed SQL engine that doesn't require schemas. It shares the same schema-free JSON model as MongoDB and Elasticsearch. No need to define and maintain schemas or transform data (ETL). Drill automatically understands the structure of the data. 

## 3. Query complex, semi-structured data in-situ
Using Drill's schema-free JSON model, you can query complex, semi-structured data in situ. No need to flatten or transform the data prior to or during query execution. Drill also provides intuitive extensions to SQL to work with nested data. Here's a simple query on a JSON file demonstrating how to access nested elements and arrays:

    SELECT * FROM (SELECT t.trans_id,
                          t.trans_info.prod_id[0] AS prod_id,
                          t.trans_info.purch_flag AS purchased
                   FROM `clicks/clicks.json` t) sq
    WHERE sq.prod_id BETWEEN 700 AND 750 AND
          sq.purchased = 'true'
    ORDER BY sq.prod_id;


## 4. Real SQL -- not "SQL-like"
Drill supports the standard SQL:2003 syntax. No need to learn a new "SQL-like" language or struggle with a semi-functional BI tool. Drill supports many data types including DATE, INTERVAL, TIMESTAMP, and VARCHAR, as well as complex query constructs such as correlated sub-queries and joins in WHERE clauses. Here is an example of a TPC-H standard query that runs in Drill:

### TPC-H query 4

    SELECT  o.o_orderpriority, COUNT(*) AS order_count
    FROM orders o
    WHERE o.o_orderdate >= DATE '1996-10-01'
          AND o.o_orderdate < DATE '1996-10-01' + INTERVAL '3' month
          AND EXISTS(
                     SELECT * FROM lineitem l 
                     WHERE l.l_orderkey = o.o_orderkey
                     AND l.l_commitdate < l.l_receiptdate
                     )
          GROUP BY o.o_orderpriority
          ORDER BY o.o_orderpriority;

## 5. Leverage standard BI tools
Drill works with standard BI tools. You can use your existing tools, such as Tableau, MicroStrategy, QlikView and Excel. 

## 6. Interactive queries on Hive tables
Apache Drill lets you leverage your investments in Hive. You can run interactive queries with Drill on your Hive tables and access all Hive input/output formats (including custom SerDes). You can join tables associated with different Hive metastores, and you can join a Hive table with an HBase table or a directory of log files. Here's a simple query in Drill on a Hive table:

    SELECT `month`, state, sum(order_total) AS sales
    FROM hive.orders 
    GROUP BY `month`, state
    ORDER BY 3 DESC LIMIT 5;


## 7. Access multiple data sources
Drill is extensible. You can connect Drill out-of-the-box to file systems (local or distributed, such as S3 and HDFS), HBase and Hive. You can implement a storage plugin to make Drill work with any other data source. Drill can combine data from multiple data sources on the fly in a single query, with no centralized metadata definitions. Here's a query that combines data from a Hive table, an HBase table (view) and a JSON file:

    SELECT custview.membership, sum(orders.order_total) AS sales
    FROM hive.orders, custview, dfs.`clicks/clicks.json` c 
    WHERE orders.cust_id = custview.cust_id AND orders.cust_id = c.user_info.cust_id 
    GROUP BY custview.membership
    ORDER BY 2;

## 8. User-Defined Functions (UDFs) for Drill and Hive
Drill exposes a simple, high-performance Java API to build [custom user-defined functions]({{ site.baseurl }}/docs/develop-custom-functions/) (UDFs) for adding your own business logic to Drill.  Drill also supports Hive UDFs. If you have already built UDFs in Hive, you can reuse them with Drill with no modifications. 


## 9. High performance
Drill is designed from the ground up for high throughput and low latency. It doesn't use a general purpose execution engine like MapReduce, Tez or Spark. As a result, Drill is flexible (schema-free JSON model) and performant. Drill's optimizer leverages rule- and cost-based techniques, as well as data locality and operator push-down, which is the capability to push down query fragments into the back-end data sources. Drill also provides a columnar and vectorized execution engine, resulting in higher memory and CPU efficiency.

## 10. Scales from a single laptop to a 1000-node cluster
Drill is available as a simple download you can run on your laptop. When you're ready to analyze larger datasets, deploy Drill on your Hadoop cluster (up to 1000 commodity servers). Drill leverages the aggregate memory in the cluster to execute queries using an optimistic pipelined model, and automatically spills to disk when the working set doesn't fit in memory.
