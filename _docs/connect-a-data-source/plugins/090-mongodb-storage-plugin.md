---
title: "MongoDB Storage Plugin"
date: 2018-11-02
parent: "Connect a Data Source"
---
## Overview

Drill supports MongoDB 3.0, providing a mongodb storage plugin to connect to MongoDB using MongoDB's latest Java driver. You can run queries
to read, but not write, Mongo data using Drill. Attempting to write data back to Mongo results in an error. You do not need any upfront schema definitions. 

{% include startnote.html %}In the following examples, you use a local instance of Drill for simplicity. {% include endnote.html %}

You can also run Drill and MongoDB together in distributed mode.

### Before You Begin

To query MongoDB with Drill, you install Drill and MongoDB, and then you import zip code aggregation data into MongoDB. 

  1. [Install Drill]({{ site.baseurl }}/docs/installing-drill-in-embedded-mode), if you do not already have it installed.
  2. [Install MongoDB](http://docs.mongodb.org/manual/installation), if you do not already have it installed.
  3. [Import the MongoDB zip code sample data set](http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set).   * Copy the `zips.json` content into a file and save it.  
     * Create `/data/db` if it doesn't already exist.
     * Make sure you have permissions to access the directories. 
     * Use Mongo Import to import `zips.json`. 

## Configuring MongoDB

Drill must be running in order to access the Web Console to configure a storage plugin configuration. Start Drill and view and enable the MongoDB storage plugin configuration as described in the following procedure: 

  1. [Start the Drill shell]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/).

     The Drill shell needs to be running to access the Drill Web Console.
  2. In the [Drill Web Console]({{ site.baseurl }}/docs/plugin-configuration-basics/#using-the-drill-web-console), select the **Storage** tab.
  4. Under Disabled Storage Plugins, select **Update** to choose the `mongo` storage plugin configuration.
  5. In the Configuration window, take a look at the default configuration:
     
        {
          "type": "mongo",
          "connection": "mongodb://localhost:27017/",
          "enabled": false
        }

     {% include startnote.html %}27017 is the default port for `mongodb` instances.{% include endnote.html %} 
     {% include startnote.html %}In some cases you will need an authentication to perform certain `mongodb` queries. You can add login and password directly to connection URL: 'mongodb://root:password@localhost:27017/'{% include endnote.html %} 
  6. Click **Enable** to enable the storage plugin.

## Querying MongoDB

In the [Drill shell]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/), set up Drill to use the zips collection you imported into MongoDB.

1. Get a list of schemas from all
Drill data sources, including MongoDB. 

        SHOW DATABASES;
   
        +---------------------+
        |     SCHEMA_NAME     |
        +---------------------+
        | INFORMATION_SCHEMA  |
        | cp.default          |
        | dfs.default         |
        | dfs.root            |
        | dfs.tmp             |
        | mongo.local         |
        | mongo.test          |
        | sys                 |
        +---------------------+
        8 rows selected (1.385 seconds)
    
2. Change the schema to mongo.text.

        USE mongo.test;

        +-------+-----------------------------------------+
        |  ok   |                 summary                 |
        +-------+-----------------------------------------+
        | true  | Default schema changed to [mongo.test]  |
        +-------+-----------------------------------------+

3. List the tables and verify that the `zips` collection appears:

        SHOW TABLES;

        +---------------+-----------------+
        | TABLE_SCHEMA  |   TABLE_NAME    |
        +---------------+-----------------+
        | mongo.test    | system.indexes  |
        | mongo.test    | zips            |
        +---------------+-----------------+
        2 rows selected (0.187 seconds)

4. Set the option to read numbers as doubles instead of as text;

        ALTER SYSTEM SET `store.mongo.read_numbers_as_double` = true;
        +-------+----------------------------------------------+
        |  ok   |                   summary                    |
        +-------+----------------------------------------------+
        | true  | store.mongo.read_numbers_as_double updated.  |
        +-------+----------------------------------------------+
        1 row selected (0.078 seconds)



### Example Queries

**Example 1: View the zips Collection**

    SELECT * FROM zips LIMIT 10;

    +---------------+-------------------------+--------+--------+
    |     city      |           loc           |  pop   | state  |
    +---------------+-------------------------+--------+--------+
    | AGAWAM        | [-72.622739,42.070206]  | 15338  | MA     |
    | CUSHMAN       | [-72.51565,42.377017]   | 36963  | MA     |
    | BELCHERTOWN   | [-72.410953,42.275103]  | 10579  | MA     |
    | BLANDFORD     | [-72.936114,42.182949]  | 1240   | MA     |
    | BRIMFIELD     | [-72.188455,42.116543]  | 3706   | MA     |
    | CHESTERFIELD  | [-72.833309,42.38167]   | 177    | MA     |
    | BARRE         | [-72.108354,42.409698]  | 4546   | MA     |
    | CHICOPEE      | [-72.607962,42.162046]  | 23396  | MA     |
    | CHICOPEE      | [-72.576142,42.176443]  | 31495  | MA     |
    | CHESTER       | [-72.988761,42.279421]  | 1688   | MA     |
    +---------------+-------------------------+--------+--------+
    10 rows selected (0.444 seconds)


**Example 2: Aggregation**

```
SELECT city, avg(pop) FROM zips GROUP BY city LIMIT 10; 

+---------------+---------------------+
|     city      |       EXPR$1        |
+---------------+---------------------+
| AGAWAM        | 15338.0             |
| CUSHMAN       | 18649.5             |
| BELCHERTOWN   | 10579.0             |
| BLANDFORD     | 1240.0              |
| BRIMFIELD     | 2441.5              |
| CHESTERFIELD  | 9988.857142857143   |
| BARRE         | 9770.0              |
| CHICOPEE      | 27445.5             |
| CHESTER       | 7285.0952380952385  |
| WESTOVER AFB  | 1764.0              |
+---------------+---------------------+
10 rows selected (1.664 seconds)
```

**Example 3: Nested Data Column Array**

    0: jdbc:drill:zk=local> SELECT loc FROM zips LIMIT 10;
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
        
    0: jdbc:drill:zk=local> SELECT loc[0] FROM zips LIMIT 10;
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

You can query MongoDB through standard
BI tools, such as Tableau and SQuirreL. For information about Drill ODBC and JDBC drivers, refer to [Drill Interfaces]({{ site.baseurl }}/docs/odbc-jdbc-interfaces).
