---
title: "MongoDB Plugin for Apache Drill"
parent: "Connect to Data Sources"
---
## Overview

You can leverage the power of Apache Drill to query data without any upfront
schema definitions. Drill enables you to create an architecture that works
with nested and dynamic schemas, making it the perfect SQL query tool to use
on NoSQL databases, such as MongoDB.

As of Apache Drill 0.6, you can configure MongoDB as a Drill data source.
Drill provides a mongodb format plugin to connect to MongoDB, and run queries
on the data using ANSI SQL.

This tutorial assumes that you have Drill installed locally (embedded mode),
as well as MongoDB. Examples in this tutorial use zip code aggregation data
provided by MongDB. Before You Begin provides links to download tools and data
used throughout the tutorial.

**Note:** A local instance of Drill is used in this tutorial for simplicity. You can also run Drill and MongoDB together in distributed mode.

### Before You Begin

Before you can query MongoDB with Drill, you must have Drill and MongoDB
installed on your machine. You may also want to import the MongoDB zip code
data to run the example queries on your machine.

  1. [Install Drill](/drill/docs/installing-drill-in-embedded-mode), if you do not already have it installed on your machine.
  2. [Install MongoDB](http://docs.mongodb.org/manual/installation), if you do not already have it installed on your machine.
  3. [Import the MongoDB zip code sample data set](http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set). You can use Mongo Import to get the data. 

## Configuring MongoDB

Start Drill and configure the MongoDB storage plugin instance in the Drill Web
UI to connect to Drill. Drill must be running in order to access the Web UI.

Complete the following steps to configure MongoDB as a data source for Drill:

  1. Navigate to `<drill_installation_directory>/drill-<version>,` and enter the following command to invoke SQLLine and start Drill:

        bin/sqlline -u jdbc:drill:zk=local -n admin -p admin
     When Drill starts, the following prompt appears: `0: jdbc:drill:zk=local>`

     Do not enter any commands. You will return to the command prompt after
completing the configuration in the Drill Web UI.
  2. Open a browser window, and navigate to the Drill Web UI at `http://localhost:8047`.
  3. In the navigation bar, click **Storage**.
  4. Under Disabled Storage Plugins, select **Update** next to the `mongo` instance if the instance exists. If the instance does not exist, create an instance for MongoDB.
  5. In the Configuration window, verify that `"enabled"` is set to ``"true."``

     **Example**
     
        {
          "type": "mongo",
          "connection": "mongodb://localhost:27017/",
          "enabled": true
        }

     **Note:** 27017 is the default port for `mongodb` instances. 
  6. Click **Enable** to enable the instance, and save the configuration.
  7. Navigate back to the Drill command line so you can query MongoDB.

## Querying MongoDB

You can issue the `SHOW DATABASES `command to see a list of databases from all
Drill data sources, including MongoDB. If you downloaded the zip codes file,
you should see `mongo.zipdb` in the results.

    0: jdbc:drill:zk=local> SHOW DATABASES;
    +-------------+
    | SCHEMA_NAME |
    +-------------+
    | dfs.default |
    | dfs.root    |
    | dfs.tmp     |
    | sys         |
    | mongo.zipdb |
    | cp.default  |
    | INFORMATION_SCHEMA |
    +-------------+

If you want all queries that you submit to run on `mongo.zipdb`, you can issue
the `USE` command to change schema.

### Example Queries

The following example queries are included for reference. However, you can use
the SQL power of Apache Drill directly on MongoDB. For more information,
refer to the [Apache Drill SQL
Reference](/drill/docs/sql-reference).

**Example 1: View mongo.zipdb Dataset**

    0: jdbc:drill:zk=local> SELECT * FROM zipcodes LIMIT 10;
    +------------+
    |     *      |
    +------------+
    | { "city" : "AGAWAM" , "loc" : [ -72.622739 , 42.070206] , "pop" : 15338 , "state" : "MA"} |
    | { "city" : "CUSHMAN" , "loc" : [ -72.51565 , 42.377017] , "pop" : 36963 , "state" : "MA"} |
    | { "city" : "BARRE" , "loc" : [ -72.108354 , 42.409698] , "pop" : 4546 , "state" : "MA"} |
    | { "city" : "BELCHERTOWN" , "loc" : [ -72.410953 , 42.275103] , "pop" : 10579 , "state" : "MA"} |
    | { "city" : "BLANDFORD" , "loc" : [ -72.936114 , 42.182949] , "pop" : 1240 , "state" : "MA"} |
    | { "city" : "BRIMFIELD" , "loc" : [ -72.188455 , 42.116543] , "pop" : 3706 , "state" : "MA"} |
    | { "city" : "CHESTER" , "loc" : [ -72.988761 , 42.279421] , "pop" : 1688 , "state" : "MA"} |
    | { "city" : "CHESTERFIELD" , "loc" : [ -72.833309 , 42.38167] , "pop" : 177 , "state" : "MA"} |
    | { "city" : "CHICOPEE" , "loc" : [ -72.607962 , 42.162046] , "pop" : 23396 , "state" : "MA"} |
    | { "city" : "CHICOPEE" , "loc" : [ -72.576142 , 42.176443] , "pop" : 31495 , "state" : "MA"} |

**Example 2: Aggregation**

    0: jdbc:drill:zk=local> select state,city,avg(pop)
    +------------+------------+------------+
    |   state    |    city    |   EXPR$2   |
    +------------+------------+------------+
    | MA         | AGAWAM     | 15338.0    |
    | MA         | CUSHMAN    | 36963.0    |
    | MA         | BARRE      | 4546.0     |
    | MA         | BELCHERTOWN | 10579.0   |
    | MA         | BLANDFORD  | 1240.0     |
    | MA         | BRIMFIELD  | 3706.0     |
    | MA         | CHESTER    | 1688.0     |
    | MA         | CHESTERFIELD | 177.0    |
    | MA         | CHICOPEE   | 27445.5    |
    | MA         | WESTOVER AFB | 1764.0   |
    +------------+------------+------------+

**Example 3: Nested Data Column Array**

    0: jdbc:drill:zk=local> SELECT loc FROM zipcodes LIMIT 10;
    +------------------------+
    |    loc                 |
    +------------------------+
    | [-72.622739,42.070206] |
    | [-72.51565,42.377017]  |
    | [-72.108354,42.409698] |
    | [-72.410953,42.275103] |
    | [-72.936114,42.182949] |
    | [-72.188455,42.116543] |
    | [-72.988761,42.279421] |
    | [-72.833309,42.38167]  |
    | [-72.607962,42.162046] |
    | [-72.576142,42.176443] |
    +------------------------+
        
    0: jdbc:drill:zk=local> SELECT loc[0] FROM zipcodes LIMIT 10;
    +------------+
    |   EXPR$0   |
    +------------+
    | -72.622739 |
    | -72.51565  |
    | -72.108354 |
    | -72.410953 |
    | -72.936114 |
    | -72.188455 |
    | -72.988761 |
    | -72.833309 |
    | -72.607962 |
    | -72.576142 |
    +------------+

## Using ODBC/JDBC Drivers

You can leverage the power of Apache Drill to query MongoDB through standard
BI tools, such as Tableau and SQuirreL.

For information about Drill ODBC and JDBC drivers, refer to [Drill Interfaces](/drill/docs/odbc-jdbc-interfaces).
