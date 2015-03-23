---
title: "File System Storage Plugin"
parent: "Storage Plugin Configuration"
---
[Previous](/docs/workspaces)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Back to Table of Contents](/docs)<code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code>[Next](/docs/hbase-storage-plugin)

You can register a storage plugin instance that connects Drill to a local file
system or a distributed file system registered in `core-site.xml`, such as S3
or HDFS. When you register a storage plugin instance for a file system,
provide a unique name for the instance, and identify the type as “`file`”. By
default, Drill includes an instance named `dfs` that points to the local file
system on your machine. You can update this configuration to point to a
distributed file system or you can create a new instance to point to a
distributed file system.

To register a local or a distributed file system with Apache Drill, complete
the following steps:

  1. Navigate to `[http://localhost:8047](http://localhost:8047/)`, and select the **Storage** tab.
  2. In the New Storage Plugin window, enter a unique name and then click **Create**.
  3. In the Configuration window, provide the following configuration information for the type of file system that you are configuring as a data source.
     1. Local file system example:

            {
              "type": "file",
              "enabled": true,
              "connection": "file:///",
              "workspaces": {
                "root": {
                  "location": "/user/max/donuts",
                  "writable": false,
                  "defaultinputformat": null
                 }
              },
                 "formats" : {
                   "json" : {
                     "type" : "json"
                   }
                 }
              }
     2. Distributed file system example:
    
            {
              "type" : "file",
              "enabled" : true,
              "connection" : "hdfs://10.10.30.156:8020/",
              "workspaces" : {
                "root : {
                  "location" : "/user/root/drill",
                  "writable" : true,
                  "defaultinputformat" : "null"
                }
              },
              "formats" : {
                "json" : {
                  "type" : "json"
                }
              }
            }

      To connect to a Hadoop file system, you must include the IP address of the
name node and the port number.
  4. Click **Enable**.

Once you have configured a storage plugin instance for the file system, you
can issue Drill queries against it.