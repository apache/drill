---
title: "File System Storage Plugin"
parent: "Storage Plugin Configuration"
---
You can register a storage plugin instance that connects Drill to a local file system or to a distributed file system registered in `core-site.xml`, such as S3
or HDFS. By
default, Apache Drill includes an storage plugin named `dfs` that points to the local file
system on your machine by default. 

## Connecting Drill to a File System

In a Drill cluster, you typically do not query the local file system, but instead place files on the distributed file system. You configure the connection property of the storage plugin workspace to connect Drill to a distributed file system. For example, the following connection properties connect Drill to an HDFS or MapR-FS cluster:

* HDFS  
  `"connection": "hdfs://<IP Address>:<Port>/"`  
* MapR-FS Remote Cluster  
  `"connection": "maprfs://<IP Address>/"`  

To register a local or a distributed file system with Apache Drill, complete
the following steps:

  1. Navigate to `[http://localhost:8047](http://localhost:8047/)`, and select the **Storage** tab.
  2. In the New Storage Plugin window, enter a unique name and then click **Create**.
  3. In the Configuration window, provide the following configuration information for the type of file system that you are configuring as a data source.
     * Local file system example:

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
     * Distributed file system example:
    
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

      To connect to a Hadoop file system, you include the IP address of the
name node and the port number.
  4. Click **Enable**.

After you have configured a storage plugin instance for the file system, you
can issue Drill queries against it.

The following example shows an instance of a file type storage plugin with a
workspace named `json_files` configured to point Drill to the
`/users/max/drill/json/` directory in the local file system `(dfs)`:

    {
      "type" : "file",
      "enabled" : true,
      "connection" : "file:///",
      "workspaces" : {
        "json_files" : {
          "location" : "/users/max/drill/json/",
          "writable" : false,
          "defaultinputformat" : json
       } 
    },

{% include startnote.html %}The `connection` parameter in the configuration above is "`file:///`", connecting Drill to the local file system (`dfs`).{% include endnote.html %}

To query a file in the example `json_files` workspace, you can issue the `USE`
command to tell Drill to use the `json_files` workspace configured in the `dfs`
instance for each query that you issue:

**Example**

    USE dfs.json_files;
    SELECT * FROM dfs.json_files.`donuts.json` WHERE type='frosted'

If the `json_files` workspace did not exist, the query would have to include the
full path to the `donuts.json` file:

    SELECT * FROM dfs.`/users/max/drill/json/donuts.json` WHERE type='frosted';