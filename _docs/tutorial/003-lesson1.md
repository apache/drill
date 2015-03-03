---
title: "Lession 1: Learn about the Data Set"
parent: "Apache Drill Tutorial"
---
## Goal

This lesson is simply about discovering what data is available, in what
format, using simple SQL SELECT statements. Drill is capable of analyzing data
without prior knowledge or definition of its schema. This means that you can
start querying data immediately (and even as it changes), regardless of its
format.

The data set for the tutorial consists of:

  * Transactional data: stored as a Hive table
  * Product catalog and master customer data: stored as MapR-DB tables
  * Clickstream and logs data: stored in the MapR file system as JSON files

## Queries in This Lesson

This lesson consists of select * queries on each data source.

## Before You Begin

### Start SQLLine

If SQLLine is not already started, use a Terminal or Command window to log
into the demo VM as root, then enter `sqlline`, as described in ["Getting to Know the Sandbox"](/docs/getting-to-know-the-drill-sandbox):

You can run queries from the `sqlline` prompt to complete the tutorial. To exit from
SQLLine, type:

    0: jdbc:drill:> !quit

Examples in this tutorial use SQLLine. You can also execute queries using the Drill Web UI.

### List the available workspaces and databases:

    0: jdbc:drill:> show databases;
    +-------------+
    | SCHEMA_NAME |
    +-------------+
    | hive.default |
    | dfs.default |
    | dfs.logs    |
    | dfs.root    |
    | dfs.views   |
    | dfs.clicks  |
    | dfs.tmp     |
    | sys         |
    | maprdb      |
    | cp.default  |
    | INFORMATION_SCHEMA |
    +-------------+
    12 rows selected

This command exposes all the metadata available from the storage
plugins configured with Drill as a set of schemas. The Hive and
MapR-DB databases, file system, and other data are configured in the file system. As
you run queries in the tutorial, you will switch among these schemas by
submitting the USE command. This behavior resembles the ability to use
different database schemas (namespaces) in a relational database system.

## Query Hive Tables

The orders table is a six-column Hive table defined in the Hive metastore.
This is a Hive external table pointing to the data stored in flat files on the
MapR file system. The orders table contains 122,000 rows.

### Set the schema to hive:

    0: jdbc:drill:> use hive;
    +------------+------------+
    | ok | summary |
    +------------+------------+
    | true | Default schema changed to 'hive' |
    +------------+------------+

You will run the USE command throughout this tutorial. The USE command sets
the schema for the current session.

### Describe the table:

You can use the DESCRIBE command to show the columns and data types for a Hive
table:

    0: jdbc:drill:> describe orders;
    +-------------+------------+-------------+
    | COLUMN_NAME | DATA_TYPE  | IS_NULLABLE |
    +-------------+------------+-------------+
    | order_id    | BIGINT     | YES         |
    | month       | VARCHAR    | YES         |
    | cust_id     | BIGINT     | YES         |
    | state       | VARCHAR    | YES         |
    | prod_id     | BIGINT     | YES         |
    | order_total | INTEGER    | YES         |
    +-------------+------------+-------------+

The DESCRIBE command returns complete schema information for Hive tables based
on the metadata available in the Hive metastore.

### Select 5 rows from the orders table:

    0: jdbc:drill:> select * from orders limit 5;
    +------------+------------+------------+------------+------------+-------------+
    |  order_id  |   month    |  cust_id   |   state    |  prod_id   | order_total |
    +------------+------------+------------+------------+------------+-------------+
    | 67212      | June       | 10001      | ca         | 909        | 13          |
    | 70302      | June       | 10004      | ga         | 420        | 11          |
    | 69090      | June       | 10011      | fl         | 44         | 76          |
    | 68834      | June       | 10012      | ar         | 0          | 81          |
    | 71220      | June       | 10018      | az         | 411        | 24          |
    +------------+------------+------------+------------+------------+-------------+

Because orders is a Hive table, you can query the data in the same way that
you would query the columns in a relational database table. Note the use of
the standard LIMIT clause, which limits the result set to the specified number
of rows. You can use LIMIT with or without an ORDER BY clause.

Drill provides seamless integration with Hive by allowing queries on Hive
tables defined in the metastore with no extra configuration. Note that Hive is
not a prerequisite for Drill, but simply serves as a storage plugin or data
source for Drill. Drill also lets users query all Hive file formats (including
custom serdes). Additionally, any UDFs defined in Hive can be leveraged as
part of Drill queries.

Because Drill has its own low-latency SQL query execution engine, you can
query Hive tables with high performance and support for interactive and ad-hoc
data exploration.

## Query MapR-DB and HBase Tables

The customers and products tables are MapR-DB tables. MapR-DB is an enterprise
in-Hadoop NoSQL database. It exposes the HBase API to support application
development. Every MapR-DB table has a row_key, in addition to one or more
column families. Each column family contains one or more specific columns. The
row_key value is a primary key that uniquely identifies each row.

Drill allows direct queries on MapR-DB and HBase tables. Unlike other SQL on
Hadoop options, Drill requires no overlay schema definitions in Hive to work
with this data. Think about a MapR-DB or HBase table with thousands of
columns, such as a time-series database, and the pain of having to manage
duplicate schemas for it in Hive!

### Products Table

The products table has two column families.

<table ><colgroup><col /><col /></colgroup><tbody><tr><td ><span style="color: rgb(0,0,0);">Column Family</span></td><td ><span style="color: rgb(0,0,0);">Columns</span></td></tr><tr><td ><span style="color: rgb(0,0,0);">details</span></td><td ><span style="color: rgb(0,0,0);">name</br></span><span style="color: rgb(0,0,0);">category</span></td></tr><tr><td ><span style="color: rgb(0,0,0);">pricing</span></td><td ><span style="color: rgb(0,0,0);">price</span></td></tr></tbody></table>  
The products table contains 965 rows.

### Customers Table

The Customers table has three column families.

<table ><colgroup><col /><col /></colgroup><tbody><tr><td ><span style="color: rgb(0,0,0);">Column Family</span></td><td ><span style="color: rgb(0,0,0);">Columns</span></td></tr><tr><td ><span style="color: rgb(0,0,0);">address</span></td><td ><span style="color: rgb(0,0,0);">state</span></td></tr><tr><td ><span style="color: rgb(0,0,0);">loyalty</span></td><td ><span style="color: rgb(0,0,0);">agg_rev</br></span><span style="color: rgb(0,0,0);">membership</span></td></tr><tr><td ><span style="color: rgb(0,0,0);">personal</span></td><td ><span style="color: rgb(0,0,0);">age</br></span><span style="color: rgb(0,0,0);">gender</span></td></tr></tbody></table>  
  
The customers table contains 993 rows.

### Set the workspace to maprdb:

    0: jdbc:drill:> use maprdb;
    +------------+------------+
    | ok | summary |
    +------------+------------+
    | true | Default schema changed to 'maprdb' |
    +------------+------------+

### Describe the tables:

    0: jdbc:drill:> describe customers;
    +-------------+------------+-------------+
    | COLUMN_NAME | DATA_TYPE  | IS_NULLABLE |
    +-------------+------------+-------------+
    | row_key     | ANY        | NO          |
    | address     | (VARCHAR(1), ANY) MAP | NO          |
    | loyalty     | (VARCHAR(1), ANY) MAP | NO          |
    | personal    | (VARCHAR(1), ANY) MAP | NO          |
    +-------------+------------+-------------+
 
    0: jdbc:drill:> describe products;
    +-------------+------------+-------------+
    | COLUMN_NAME | DATA_TYPE  | IS_NULLABLE |
    +-------------+------------+-------------+
    | row_key     | ANY        | NO          |
    | details     | (VARCHAR(1), ANY) MAP | NO          |
    | pricing     | (VARCHAR(1), ANY) MAP | NO          |
    +-------------+------------+-------------+

Unlike the Hive example, the DESCRIBE command does not return the full schema
up to the column level. Wide-column NoSQL databases such as MapR-DB and HBase
can be schema-less by design; every row has its own set of column name-value
pairs in a given column family, and the column value can be of any data type,
as determined by the application inserting the data.

A “MAP” complex type in Drill represents this variable column name-value
structure, and “ANY” represents the fact that the column value can be of any
data type. Observe the row_key, which is also simply bytes and has the type
ANY.

### Select 5 rows from the products table:

    0: jdbc:drill:> select * from products limit 5;
    +------------+------------+------------+
    | row_key | details | pricing |
    +------------+------------+------------+
    | [B@a1a3e25 | {"category":"bGFwdG9w","name":"IlNvbnkgbm90ZWJvb2si"} | {"price":"OTU5"} |
    | [B@103a43af | {"category":"RW52ZWxvcGVz","name":"IzEwLTQgMS84IHggOSAxLzIgUHJlbWl1bSBEaWFnb25hbCBTZWFtIEVudmVsb3Blcw=="} | {"price":"MT |
    | [B@61319e7b | {"category":"U3RvcmFnZSAmIE9yZ2FuaXphdGlvbg==","name":"MjQgQ2FwYWNpdHkgTWF4aSBEYXRhIEJpbmRlciBSYWNrc1BlYXJs"} | {"price" |
    | [B@9bcf17 | {"category":"TGFiZWxz","name":"QXZlcnkgNDk4"} | {"price":"Mw=="} |
    | [B@7538ef50 | {"category":"TGFiZWxz","name":"QXZlcnkgNDk="} | {"price":"Mw=="} |

Given that Drill requires no up front schema definitions indicating data
types, the query returns the raw byte arrays for column values, just as they
are stored in MapR-DB (or HBase). Observe that the column families (details
and pricing) have the map data type and appear as JSON strings.

In Lesson 2, you will use CAST functions to return typed data for each column.

### Select 5 rows from the customers table:


    +0: jdbc:drill:> select * from customers limit 5;
    +------------+------------+------------+------------+
    | row_key | address | loyalty | personal |
    +------------+------------+------------+------------+
    | [B@284bae62 | {"state":"Imt5Ig=="} | {"agg_rev":"IjEwMDEtMzAwMCI=","membership":"ImJhc2ljIg=="} | {"age":"IjI2LTM1Ig==","gender":"Ik1B |
    | [B@7ffa4523 | {"state":"ImNhIg=="} | {"agg_rev":"IjAtMTAwIg==","membership":"ImdvbGQi"} | {"age":"IjI2LTM1Ig==","gender":"IkZFTUFMRSI= |
    | [B@7d13e79 | {"state":"Im9rIg=="} | {"agg_rev":"IjUwMS0xMDAwIg==","membership":"InNpbHZlciI="} | {"age":"IjI2LTM1Ig==","gender":"IkZFT |
    | [B@3a5c7df1 | {"state":"ImtzIg=="} | {"agg_rev":"IjMwMDEtMTAwMDAwIg==","membership":"ImdvbGQi"} | {"age":"IjUxLTEwMCI=","gender":"IkZF |
    | [B@e507726 | {"state":"Im5qIg=="} | {"agg_rev":"IjAtMTAwIg==","membership":"ImJhc2ljIg=="} | {"age":"IjIxLTI1Ig==","gender":"Ik1BTEUi" |
    +------------+------------+------------+------------+

Again the table returns byte data that needs to be cast to readable data
types.

## Query the File System

Along with querying a data source with full schemas (such as Hive) and partial
schemas (such as MapR-DB and HBase), Drill offers the unique capability to
perform SQL queries directly on file system. The file system could be a local
file system, or a distributed file system such as MapR-FS, HDFS, or S3.

In the context of Drill, a file or a directory is considered as synonymous to
a relational database “table.” Therefore, you can perform SQL operations
directly on files and directories without the need for up-front schema
definitions or schema management for any model changes. The schema is
discovered on the fly based on the query. Drill supports queries on a variety
of file formats including text, CSV, Parquet, and JSON.

In this example, the clickstream data coming from the mobile/web applications
is in JSON format. The JSON files have the following structure:

    {"trans_id":31920,"date":"2014-04-26","time":"12:17:12","user_info":{"cust_id":22526,"device":"IOS5","state":"il"},"trans_info":{"prod_id":[174,2],"purch_flag":"false"}}
    {"trans_id":31026,"date":"2014-04-20","time":"13:50:29","user_info":{"cust_id":16368,"device":"AOS4.2","state":"nc"},"trans_info":{"prod_id":[],"purch_flag":"false"}}
    {"trans_id":33848,"date":"2014-04-10","time":"04:44:42","user_info":{"cust_id":21449,"device":"IOS6","state":"oh"},"trans_info":{"prod_id":[582],"purch_flag":"false"}}


The clicks.json and clicks.campaign.json files contain metadata as part of the
data itself (referred to as “self-describing” data). Also note that the data
elements are complex, or nested. The initial queries below do not show how to
unpack the nested data, but they show that easy access to the data requires no
setup beyond the definition of a workspace.

### Query nested clickstream data

#### Set the workspace to dfs.clicks:

     0: jdbc:drill:> use dfs.clicks;
    +------------+------------+
    | ok | summary |
    +------------+------------+
    | true | Default schema changed to 'dfs.clicks' |
    +------------+------------+

In this case, setting the workspace is a mechanism for making queries easier
to write. When you specify a file system workspace, you can shorten references
to files in your queries. Instead of having to provide the
complete path to a file, you can provide the path relative to a directory
location specified in the workspace. For example:

    "location": "/mapr/demo.mapr.com/data/nested"

Any file or directory that you want to query in this path can be referenced
relative to this path. The clicks directory referred to in the following query
is directly below the nested directory.

#### Select 2 rows from the clicks.json file:

    0: jdbc:drill:> select * from `clicks/clicks.json` limit 2;
    +------------+------------+------------+------------+------------+
    |  trans_id  |    date    |    time    | user_info  | trans_info |
    +------------+------------+------------+------------+------------+
    | 31920      | 2014-04-26 | 12:17:12   | {"cust_id":22526,"device":"IOS5","state":"il"} | {"prod_id":[174,2],"purch_flag":"false"} |
    | 31026      | 2014-04-20 | 13:50:29   | {"cust_id":16368,"device":"AOS4.2","state":"nc"} | {"prod_id":[],"purch_flag":"false"} |
    +------------+------------+------------+------------+------------+
    2 rows selected

Note that the FROM clause reference points to a specific file. Drill expands
the traditional concept of a “table reference” in a standard SQL FROM clause
to refer to a file in a local or distributed file system.

The only special requirement is the use of back ticks to enclose the file
path. This is necessary whenever the file path contains Drill reserved words
or characters.

#### Select 2 rows from the campaign.json file:

    0: jdbc:drill:> select * from `clicks/clicks.campaign.json` limit 2;
    +------------+------------+------------+------------+------------+------------+
    |  trans_id  |    date    |    time    | user_info  |  ad_info   | trans_info |
    +------------+------------+------------+------------+------------+------------+
    | 35232      | 2014-05-10 | 00:13:03   | {"cust_id":18520,"device":"AOS4.3","state":"tx"} | {"camp_id":"null"} | {"prod_id":[7,7],"purch_flag":"true"} |
    | 31995      | 2014-05-22 | 16:06:38   | {"cust_id":17182,"device":"IOS6","state":"fl"} | {"camp_id":"null"} | {"prod_id":[],"purch_flag":"false"} |
    +------------+------------+------------+------------+------------+------------+
    2 rows selected

Notice that with a select * query, any complex data types such as maps and
arrays return as JSON strings. You will see how to unpack this data using
various SQL functions and operators in the next lesson.

## Query Logs Data

Unlike the previous example where we performed queries against clicks data in
one file, logs data is stored as partitioned directories on the file system.
The logs directory has three subdirectories:

  * 2012
  * 2013
  * 2014

Each of these year directories fans out to a set of numbered month
directories, and each month directory contains a JSON file with log records
for that month. The total number of records in all log files is 48000.

The files in the logs directory and its subdirectories are JSON files. There
are many of these files, but you can use Drill to query them all as a single
data source, or to query a subset of the files.

#### Set the workspace to dfs.logs:

     0: jdbc:drill:> use dfs.logs;
    +------------+------------+
    | ok | summary |
    +------------+------------+
    | true | Default schema changed to 'dfs.logs' |
    +------------+------------+

#### Select 2 rows from the logs directory:

    0: jdbc:drill:> select * from logs limit 2;
    +------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+----------+
    | dir0 | dir1 | trans_id | date | time | cust_id | device | state | camp_id | keywords | prod_id | purch_fl |
    +------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+----------+
    | 2014 | 8 | 24181 | 08/02/2014 | 09:23:52 | 0 | IOS5 | il | 2 | wait | 128 | false |
    | 2014 | 8 | 24195 | 08/02/2014 | 07:58:19 | 243 | IOS5 | mo | 6 | hmm | 107 | false |
    +------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+----------+

Note that this is flat JSON data. The dfs.clicks workspace location property
points to a directory that contains the logs directory, making the FROM clause
reference for this query very simple. You do not have to refer to the complete
directory path on the file system.

The column names dir0 and dir1 are special Drill variables that identify
subdirectories below the logs directory. In Lesson 3, you will do more complex
queries that leverage these dynamic variables.

#### Find the total number of rows in the logs directory (all files):

    0: jdbc:drill:> select count(*) from logs;
    +------------+
    | EXPR$0 |
    +------------+
    | 48000 |
    +------------+

This query traverses all of the files in the logs directory and its
subdirectories to return the total number of rows in those files.

# What's Next

Go to [Lesson 2: Run Queries with ANSI
SQL](/drill/docs/lession-2-run-queries-with-ansi-sql).



