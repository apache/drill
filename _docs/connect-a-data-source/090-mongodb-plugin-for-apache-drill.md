---
title: "MongoDB Plugin for Apache Drill"
parent: "Connect a Data Source"
---
## Overview

Drill provides a mongodb format plugin to connect to MongoDB, and run queries
to read, but not write, the Mongo data using Drill. Attempting to write data back to Mongo results in an error. You do not need any upfront schema definitions. 

{% include startnote.html %}A local instance of Drill is used in this tutorial for simplicity. {% include endnote.html %}

You can also run Drill and MongoDB together in distributed mode.

### Before You Begin

Before you can query MongoDB with Drill, you must have Drill and MongoDB
installed on your machine. Examples in this tutorial use zip code aggregation data
provided by MongoDB that you download in the following steps:

  1. [Install Drill]({{ site.baseurl }}/docs/installing-drill-in-embedded-mode), if you do not already have it installed.
  2. [Install MongoDB](http://docs.mongodb.org/manual/installation), if you do not already have it installed.
  3. [Import the MongoDB zip code sample data set](http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set). You can use Mongo Import to get the data. 

## Configuring MongoDB

Start Drill and configure the MongoDB storage plugin instance in the Drill Web
UI to connect to Drill. Drill must be running in order to access the Web UI.

Complete the following steps to configure MongoDB as a data source for Drill:

  1. [Start the Drill shell]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/).

     The Drill shell needs to be running to access the Drill Web UI.
  2. Open a browser window, and navigate to the Drill Web UI at `http://localhost:8047`.
  3. In the navigation bar, click **Storage**.
  4. Under Disabled Storage Plugins, select **Update** next to the `mongo` storage plugin.
  5. In the Configuration window, verify that `"enabled"` is set to ``"true."``

     **Example**
     
        {
          "type": "mongo",
          "connection": "mongodb://localhost:27017/",
          "enabled": true
        }

     {% include startnote.html %}27017 is the default port for `mongodb` instances.{% include endnote.html %} 
  6. Click **Enable** to enable the storage plugin, and save the configuration.

## Querying MongoDB

In the Drill shell, you can issue the `SHOW DATABASES `command to see a list of databases from all
Drill data sources, including MongoDB. If you downloaded the zip codes file,
you should see `mongo.zipdb` in the results.

    0: jdbc:drill:zk=local> SHOW DATABASES;
    +--------------------+
    |     SCHEMA_NAME    |
    +--------------------+
    | dfs.default        |
    | dfs.root           |
    | dfs.tmp            |
    | sys                |
    | mongo.zipdb        |
    | cp.default         |
    | INFORMATION_SCHEMA |
    +--------------------+

If you want all queries that you submit to run on `mongo.zipdb`, you can issue
the `USE` command to change schema.

### Example Queries

The following example queries are included for reference. However, you can use
the SQL power of Apache Drill directly on MongoDB. For more information about,
refer to the [SQL
Reference]({{ site.baseurl }}/docs/sql-reference).

**Example 1: View mongo.zipdb Dataset**

    0: jdbc:drill:zk=local> SELECT * FROM zipcodes LIMIT 10;
    +------------------------------------------------------------------------------------------------+
    |                                           *                                                    |
    +------------------------------------------------------------------------------------------------+
    | { "city" : "AGAWAM" , "loc" : [ -72.622739 , 42.070206] , "pop" : 15338 , "state" : "MA"}      |
    | { "city" : "CUSHMAN" , "loc" : [ -72.51565 , 42.377017] , "pop" : 36963 , "state" : "MA"}      |
    | { "city" : "BARRE" , "loc" : [ -72.108354 , 42.409698] , "pop" : 4546 , "state" : "MA"}        |
    | { "city" : "BELCHERTOWN" , "loc" : [ -72.410953 , 42.275103] , "pop" : 10579 , "state" : "MA"} |
    | { "city" : "BLANDFORD" , "loc" : [ -72.936114 , 42.182949] , "pop" : 1240 , "state" : "MA"}    |
    | { "city" : "BRIMFIELD" , "loc" : [ -72.188455 , 42.116543] , "pop" : 3706 , "state" : "MA"}    |
    | { "city" : "CHESTER" , "loc" : [ -72.988761 , 42.279421] , "pop" : 1688 , "state" : "MA"}      |
    | { "city" : "CHESTERFIELD" , "loc" : [ -72.833309 , 42.38167] , "pop" : 177 , "state" : "MA"}   |
    | { "city" : "CHICOPEE" , "loc" : [ -72.607962 , 42.162046] , "pop" : 23396 , "state" : "MA"}    |
    | { "city" : "CHICOPEE" , "loc" : [ -72.576142 , 42.176443] , "pop" : 31495 , "state" : "MA"}    |

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

For information about Drill ODBC and JDBC drivers, refer to [Drill Interfaces]({{ site.baseurl }}/docs/odbc-jdbc-interfaces).
