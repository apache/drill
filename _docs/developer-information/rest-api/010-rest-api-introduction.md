---
title: "REST API Introduction"
date: 2018-02-09 00:16:00 UTC
parent: "REST API"
---

The Drill REST API provides programmatic access to Drill through the [Web Console]({{site.baseurl}}/docs/starting-the-web-console/). Using HTTP requests, you can run queries, perform storage plugin tasks, such as creating a storage plugin, obtain profiles of queries, and get current memory metrics. 

AN HTTP request uses the familiar Web Console URI:

`http://<IP address or host name>:8047`

## Getting Started

Before making HTTP requests, [start Drill]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x).

Several examples in the document use the donuts.json file. To download this file, go to [Drill test resources](https://github.com/apache/drill/blob/master/exec/java-exec/src/test/resources) page, locate donuts.json in the list of files, and download it. When using cURL, use unicode \u0027 for the single quotation mark as shown in the Query example.

This documentation presents HTTP methods in the same order as functions appear in the Web Console:

[**Query**]({{site.baseurl}}/docs/rest-api/#query)

Submit a query and return results.

[**Profiles**](({{site.baseurl}}/docs/rest-api/#profiles))

* Get the profiles of running and completed queries.  
* Get the profile of the query that has the given queryid.  
* Cancel the query that has the given queryid.  

[**Storage**]({{site.baseurl}}/docs/rest-api/#storage)

* Get the list of storage plugin names and configurations.  
* Get the definition of the named storage plugin.  
* Enable or disable the named storage plugin.  
* Create or update a storage plugin configuration.  
* Delete a storage plugin configuration.  
* Get Drillbit information, such as ports numbers.  
* Get the current memory metrics.  

[**Threads**](({{site.baseurl}}/docs/rest-api/#threads))

Get the status of threads.  

[**Options**]({{site.baseurl}}/docs/rest-api/#options)

List information about system/session options.  

<!-- * Change the value or type of the named option.   -->


## Query

Using the query method, you can programmatically run queries. 

----------

### POST /query.json

Submit a query and return results.

**Parameters**

* queryType--SQL, PHYSICAL, or LOGICAL are valid types. Use only "SQL". Other types are for internal use only.  
* query--A SQL query that runs in Drill.

**Request Body**

        {
          "queryType" : "SQL",
          "query" : "<Drill query>"
        }

**Example**

     curl -X POST -H "Content-Type: application/json" -d '{"queryType":"SQL", "query": "select * from dfs.`/Users/joe-user/apache-drill-1.4.0/sample-data/donuts.json` where name= \u0027Cake\u0027"}' http://localhost:8047/query.json

**Response Body**

     {
       "columns" : [ "id", "type", "name", "ppu", "sales", "batters", "topping", "filling" ],
       "rows" : [ {
         "id" : "0001",
         "sales" : "35",
         "name" : "Cake",
         "topping" : "[{\"id\":\"5001\",\"type\":\"None\"},{\"id\":\"5002\",\"type\":\"Glazed\"},{\"id\":\"5005\",\"type\":\"Sugar\"},{\"id\":\"5007\",\"type\":\"Powdered Sugar\"},{\"id\":\"5006\",\"type\":\"Chocolate with Sprinkles\"},{\"id\":\"5003\",\"type\":\"Chocolate\"},{\"id\":\"5004\",\"type\":\"Maple\"}]",
         "ppu" : "0.55",
         "type" : "donut",
         "batters" : "{\"batter\":[{\"id\":\"1001\",\"type\":\"Regular\"},{\"id\":\"1002\",\"type\":\"Chocolate\"},{\"id\":\"1003\",\"type\":\"Blueberry\"},{\"id\":\"1004\",\"type\":\"Devil's Food\"}]}",
         "filling" : "[]"
       } ]
     }

## Profiles

These methods get query profiles. 

----------

### GET /profiles.json

Get the profiles of running and completed queries. 

**Example**

`curl http://localhost:8047/profiles.json`

**Response Body**

        {
          "runningQueries" : [ ],
          "finishedQueries" : [ {
            "queryId" : "29b2e988-35e7-4c85-3151-32c7d3347f15",
            "time" : "11/18/2015 16:23:19",
            "location" : "http://localhost:8047/profile/29b2e988-35e7-4c85-3151-32c7d3347f15.json",
            "foreman" : "10.250.50.31",
            "query" : "select * from dfs.`/Users/joe-user/apache-drill-1.4.0/sample-data/donuts.json` where name= 'Cake'",
            "state" : "COMPLETED",
            "user" : "anonymous"
          }, 
          . . .

----------

### GET /profiles/{queryid}.json

Get the profile of the query that has the given queryid.

**Parameter**

queryid--The UUID of the query in [standard UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) format that Drill assigns to each query. 

**Example**

`curl http://localhost:8047/profiles/29b2e988-35e7-4c85-3151-32c7d3347f15.json`

**Response Body**

        {"id":{"part1":3004720672638717061,"part2":3553677414795345685},"type":1,"start":1447892599827,"end":1447892599950,"query":"select * from dfs.`/Users/joe-user/drill/apache-drill-1.4.0/sample-data/donuts.json` where name= 'Cake'","plan":"00-00    Screen : rowType = RecordType(ANY *): 
        . . ."lastUpdate":1447892599950,"lastProgress":1447892599950}]}],"user":"anonymous"}

----------

### GET /profiles/cancel/{queryid}

Cancel the query that has the given queryid.

**Parameter**

queryid--The UUID of the query in [standard UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) format that Drill assigns to each query. 

**Example**

`curl http://localhost:8047/profiles/cancel/29b2e988-35e7-4c85-3151-32c7d3347f15`

**Response Body**

        {
          "result" : "success"
        }


## Storage

The storage REST methods manage Drill storage plugin configurations.

----------

### GET /storage.json

Get the list of storage plugin names and configurations.

**Example**

`curl http://localhost:8047/storage.json`

**Response Body**

        [ {
          "name" : "cp",
          "config" : {
            "type" : "file",
            "enabled" : true,
            "connection" : "classpath:///",
            "workspaces" : null,
            "formats" : {
              "csv" : {
                "type" : "text",
                "extensions" : [ "csv" ],
                "delimiter" : ","
              },
        . . .
        {
          "name" : "mongo",
          "config" : {
            "type" : "mongo",
            "connection" : "mongodb://localhost:27017/",
            "enabled" : false
          }
        } ]

----------

### GET /storage/{name}.json

Get the definition of the named storage plugin.

**Parameter**

name--The assigned name in the storage plugin definition.

**Example**

`curl http://localhost:8047/storage/mongo.json`

**Response Body**

        {
          "name" : "mongo",
          "config" : {
            "type" : "mongo",
            "connection" : "mongodb://localhost:27017/",
            "enabled" : false
          }
        }

----------

### Get /storage/{name}/enable/{val}

Enable or disable the named storage plugin.

**Parameters**

* name--The assigned name in the storage plugin definition.
* val--Either true (to enable) or false (to disable).

**Example**

`curl http://localhost:8047/storage/mongo/enable/true`

**Response Body**

{
  "result" : "success"
}

----------

### POST /storage/{name}.json

Create or update a storage plugin configuration.

**Parameters**

name--The name of the storage plugin configuration to create or update.

**Request Body**

     {
        "name" : "<name of storage plugin configuration>",
        "config" : {
          "type" : "<type of storage plugin>",
          "enabled" : <true or false>,
            "connection" : "<type of distributed file system and address/path>",
            "workspaces" : <null or unique workspace name>,
            "formats" : <file format definitions> 
        }
     }

Valid storage plugin types include file, hbase, hive, mongo, and jdbc. Construct the request body using storage plugin [attributes and definitions]({{site.baseurl}}/docs/plugin-configuration-basics/#list-of-attributes-and-definitions). The request body overwrites the existing configuration if there is any, and therefore, must include all required attributes and definitions.

**Example**

     curl -X POST -H "Content-Type: application/json" -d '{"name":"myplugin", "config": {"type": "file", "enabled": false, "connection": "file:///", "workspaces": { "root": { "location": "/", "writable": false, "defaultInputFormat": null}}, "formats": null}}' http://localhost:8047/storage/myplugin.json

**Response Body**

{
  "result" : "success"
}

----------

### DELETE /storage/{name}.json

Delete a storage plugin configuration.

**Parameter**

name--The name of the storage plugin configuration to delete.

**Example**

`curl -X DELETE -H "Content-Type: application/json" http://localhost:8047/storage/myplugin.json`

**Response Body**

        {
          "result" : "success"
        }

## Metrics

Gets metric information.

----------

### GET /cluster.json

Get Drillbit information, such as port numbers.

**Example**

`curl http://localhost:8047/cluster.json`

**Response Body**

        [ {
          "name" : "Number of Drill Bits",
          "value" : 1
        }, {
          "name" : "Bit #0",
          "value" : "10.250.50.31 initialized"
        }, {
          "name" : "Data Port Address",
          "value" : "10.250.50.31:31012"
        }, {
          "name" : "User Port Address",
          "value" : "10.250.50.31:31010"
        }, {
          "name" : "Control Port Address",
          "value" : "10.250.50.31:31011"
        }, {
          "name" : "Maximum Direct Memory",
          "value" : 8589934592
        } ]

----------

### GET /status

Get the status of Drill. 

**Example**

`curl http://localhost:8047/status`

**Response Body**

          <!DOCTYPE html>
          . . .

            <div class="alert alert-success">
              <strong>Running!</strong>
          . . .

          </html>

----------

### GET /status/metrics

Get the current memory metrics.

**Example**

`curl http://localhost:8047/status/metrics`

**Response Body**

        {"version":"3.0.0","gauges":{"PS-MarkSweep.count":{"value":0},"PS-MarkSweep.time":{"value":0},"PS-Scavenge.count":{"value":1},"PS-Scavenge.time":{"value":74},"blocked.
         . . .
        ,"drill.allocator.normal.hist":{"count":84,"max":1024,"mean":273.6666666666667,"min":12,"p50":256.0,"p75":448.0,"p95":896.0,"p98":1024.0,"p99":1024.0,"p999":1024.0,"stddev":248.17013855555368}},"meters":{},"timers":{}}

## Threads

Get information about threads.

----------

### GET /status/threads

Get the status of threads.

**Example**

`curl http://localhost:8047/status/threads`

**Response Body**

        main id=1 state=RUNNABLE (running in native)
            at java.io.FileInputStream.read0(Native Method)
            at java.io.FileInputStream.read(FileInputStream.java:210)
        . . .

        threadDeathWatcher-2-1 id=49 state=TIMED_WAITING
            at java.lang.Thread.sleep(Native Method)
            at io.netty.util.ThreadDeathWatcher$Watcher.run(ThreadDeathWatcher.java:137)
            at io.netty.util.concurrent.DefaultThreadFactory$DefaultRunnableDecorator.run(DefaultThreadFactory.java:137)
            at java.lang.Thread.run(Thread.java:745)

        Scheduler-1174655113 id=50 state=TIMED_WAITING
            - waiting on <0x738fda67> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
            - locked <0x738fda67> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
        . . .

## Options
This method gets and sets system options.

----------

### GET /options.json

List the name, default, and data type of the system and session options.

**Example**

`curl http://localhost:8047/options.json`

**Response Body**

        [ {
          "name" : "planner.broadcast_factor",
          "value" : 1.0,
          "type" : "SYSTEM",
          "kind" : "DOUBLE"
        }, 

        . . .

        {
          "name" : "store.parquet.compression",
          "value" : "snappy",
          "type" : "SYSTEM",
          "kind" : "STRING"
        },

        . . .

        {
          "name" : "exec.enable_union_type",
          "value" : false,
          "type" : "SYSTEM",
          "kind" : "BOOLEAN"
        } ]

<!-- ----------

### POST /option/{optionName}

Change the value or type of the named option. 

**Parameter**

optionName--The name of a [Drill system option]({{site.baseurl}}/docs/configuration-options-introduction/#system-options).

**Request Body**

Enclose option values of kind STRING in double quotation marks.

        {
          "name" : "<option name>",
          "value" : ["]<new value>["],
          "type" : "<SYSTEM or SESSION>",
          "kind" : "<option data type>"
        }

**Example**

        curl -X POST -H "Content-Type: application/json" -d '{"name" : "store.json.all_text_mode", "value" : true, "type" : "SYSTEM", "kind" : "BOOLEAN"}' http://localhost:8047/option/store.json.all_text_mode


**Response Body**

        {
          "result" : "success"
        }

 -->


