---
title: "LTSV Format Plugin"
date: 2019-04-08
parent: "Connect a Data Source"
---

Starting in Drill 1.16, Drill provides a LTSV (Labeled Tab-separated Values) format plugin, which enables Drill to query files with the .LTSV file extension. LTSV is a text file format. Each line in a LTSV file has the following structure:  

	time:<value> TAB host:<value> TAB forwardedfor:<value> TAB req:<value> TAB status:<value> TAB size:<value> TAB referer:<value> TAB ua:<value> TAB reqtime:<value> TAB apptime:<value> TAB vhost:<value> NEWLINE

For more information about the LTSV file format, see [LTSV (Labeled Tab-separated Values)](http://ltsv.org/).  

##Configuring the LTSV Format Plugin  

Add the LTSV format to the dfs storage plugin configuration, as shown:  

	"formats": {
	    "ltsv": {
	      "type": "ltsv",
	      "extensions": [
	        "ltsv"
	      ]
	    },
	    ...
	  }

## Example: Querying a LTSV File  

Each line in a LTSV file has the following structure:

	time:<value> TAB host:<value> TAB forwardedfor:<value> TAB req:<value> TAB status:<value> TAB size:<value> TAB referer:<value> TAB ua:<value> TAB reqtime:<value> TAB apptime:<value> TAB vhost:<value> NEWLINE


Sample data:

	time:30/Nov/2016:00:55:08 +0900<TAB>host:xxx.xxx.xxx.xxx<TAB>forwardedfor:-<TAB>req:GET /v1/xxx HTTP/1.1<TAB>status:200<TAB>size:4968<TAB>referer:-<TAB>ua:Java/1.8.0_131<TAB>reqtime:2.532<TAB>apptime:2.532<TAB>vhost:api.example.com
	time:30/Nov/2016:00:56:37 +0900<TAB>host:xxx.xxx.xxx.xxx<TAB>forwardedfor:-<TAB>req:GET /v1/yyy HTTP/1.1<TAB>status:200<TAB>size:412<TAB>referer:-<TAB>ua:Java/1.8.0_201<TAB>reqtime:3.580<TAB>apptime:3.580<TAB>vhost:api.example.com



The following query selects the second row from the sample data shown:


	0: jdbc:drill:zk=local> SELECT * FROM dfs.`/tmp/sample.ltsv` WHERE reqtime > 3.0;
	+-----------------------------+------------------+---------------+-----------------------+---------+-------+----------+-----------------+----------+----------+------------------+
	|            time             |       host       | forwardedfor  |          req          | status  | size  | referer  |       ua        | reqtime  | apptime  |      vhost       |
	+-----------------------------+------------------+---------------+-----------------------+---------+-------+----------+-----------------+----------+----------+------------------+
	| 30/Nov/2016:00:56:37 +0900  | xxx.xxx.xxx.xxx  | -             | GET /v1/yyy HTTP/1.1  | 200     | 412   | -        | Java/1.8.0_201  | 3.580    | 3.580    | api.example.com  |
	+-----------------------------+------------------+---------------+-----------------------+---------+-------+----------+-----------------+----------+----------+------------------+
	
	
