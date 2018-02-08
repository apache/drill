---
title: "OpenTSDB Storage Plugin"
date: 2018-02-06 21:49:40 UTC
parent: "Connect a Data Source"
---

As of Drill 1.12, Drill provides a storage plugin for OpenTSDB. The OpenTSDB storage plugin uses the REST API with OpenTSDB. 

When you install Drill, a preconfigured OpenTSDB storage plugin is available on the Storage page in the Drill Web Console. Once you enable the storage plugin, you can query OpenTSDB from Drill.

The following sections describe how to enable the OpenTSDB storage plugin in Drill and the types of queries Drill currently supports on OpenTSDB. You can refer to the [README](https://github.com/apache/drill/blob/master/contrib/storage-opentsdb/README.md) and [OpenTSDB](http://opentsdb.net/) for additional information.  

##Enabling the OpenTSDB Storage Plugin  

To enable the OpenTSDB storage plugin, enter the following URL in the address bar of your browser to access the Storage page in the Drill Web Console:
http://<drill-node-ip-address>:8047/storage/

In the Disabled Storage Plugins section, click **Enable** next to OpenTSDB. OpenTSDB moves to the Enabled Storage Plugins section. Click **Update** to see the default configuration:

       {
         "type": "openTSDB",
         "connection": "http://localhost:4242",
         "enabled": true
       }

The URL, http://localhost:4242, works for TSD running locally. If you have TSD running on another machine, change the URL to reflect that. Verify that “enabled” is set to true.
Click **Update** if you edit the configuration and then click **Back** to return to the Storage page.  

##Querying OpenTSDB  
When querying OpenTSDB, you must include the required request parameters in the FROM clause of queries, using commas to separate the parameters, as shown in the following example: 

       SELECT * FROM openTSDB.`(metric=warp.speed.test, start=47y-ago, aggregator=sum)`;  


In addition to the required request parameters, you can also include some optional request parameters in queries.  


###Required Request Parameters  

The following table lists the required request parameters:  

| **Name**       | **Data Type**       | **Description**                                                        |
|------------|-----------------|--------------------------------------------------------------------|
| metric     | string          | The name of a metric stored in the system                          |
| start      | string, integer | The start time for the query; a relative or   absolute timestamp.  |
| aggregator | string          | The name of an aggregation function to use.                        |  

###Optional Request Parameters  

The following table lists optional request parameters:  
| **Name**       | **Data Type**       | **Description**                                                                                                                                                                                                                                                          |
|------------|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| downsample | string          | An optional downsampling function to reduce the   amount of data returned.                                                                                                                                                                                           |
| end        | string, integer | An end time for the query. If not supplied, the   TSD assumes the local system time on the server. If this parameter is not   specified, null is sent to the database in this field, but in this case the   database assumes the local system time on the server.    |  

###Supported Aggregators  

For a list of supported aggegrators, see [Aggregators](http://opentsdb.net/docs/build/html/user_guide/query/aggregators.html).    

###Supported Dates and Times  

For a list of supported dates and times, see [Dates](http://opentsdb.net/docs/build/html/user_guide/query/dates.html).  

##Examples of Queries that Drill Supports on OpenTSDB 

Drill currently supports the following types of queries on the OpenTSDB:

       USE openTSDB;
       //Switches Drill to the OpenTSDB schema so that Drill only queries OpenTSDB.

       SHOW tables; 
       //Prints the available metrics. The max number of the printed results is an Integer.MAX value.  

       SELECT * FROM openTSDB.`(metric=warp.speed.test, start=47y-ago, aggregator=sum)`;
       //Returns aggregated elements from the warp.speed.test table from 47y-ago.  

       SELECT * FROM openTSDB.`(metric=warp.speed.test, aggregator=avg, start=47y-ago)`;
       //Returns aggregated elements from the warp.speed.test table.  

       SELECT `timestamp`, sum(`aggregated value`) FROM openTSDB.`(metric=warp.speed.test, aggregator=avg, start=47y-ago)` GROUP BY `timestamp`;
       //Returns aggregated and grouped value by standard Drill functions from the warp.speed.test table with the custom aggregator.  

       SELECT * FROM openTSDB.`(metric=warp.speed.test, aggregator=avg, start=47y-ago, downsample=5m-avg)`;
       //Returns aggregated data limited by the downsample.  

       SELECT * FROM openTSDB.`(metric=warp.speed.test, aggregator=avg, start=47y-ago, end=1407165403000)`;
       //Returns aggregated data limited by the end time.



