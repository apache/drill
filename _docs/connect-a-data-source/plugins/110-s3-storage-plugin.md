---
title: "S3 Storage Plugin"
parent: "Connect a Data Source"
---
Drill works with data stored in the cloud. With a few simple steps, you can configure the S3 storage plugin for Drill and be off to the races running queries.

## Connecting Drill to S3

Starting with version 1.3.0, Drill has the ability to query files stored on Amazon's S3 cloud storage using the S3a library. This is important, because S3a adds support for files bigger than 5 gigabytes (these were unsupported using Drill's previous S3n interface).

There are two simple steps to follow: (1) provide your AWS credentials (2) configure S3 storage plugin with S3 bucket

#### (1) AWS credentials

To enable Drill's S3a support, edit the file conf/core-site.xml in your Drill install directory, replacing the text ENTER_YOUR_ACESSKEY and ENTER_YOUR_SECRETKEY with your AWS credentials.

```
<configuration>

  <property>
    <name>fs.s3a.access.key</name>
    <value>ENTER_YOUR_ACCESSKEY</value>
  </property>

  <property>
    <name>fs.s3a.secret.key</name>
    <value>ENTER_YOUR_SECRETKEY</value>
  </property>

</configuration>
```

#### (2) Configure S3 Storage Plugin

Enable S3 storage plugin if you already have one configured or you can add a new plugin by following these steps:

1. Point your browser to http://<host>:8047 and select the 'Storage' tab. (Note: on a single machine system, you'll need to run drill-embedded before you can access the web console site)
2. Duplicate the 'dfs' plugin. To do this, hit 'Update' next to 'dfs,' and then copy the JSON text that appears.
3. Create a new storage plugin, and paste in the 'dfs' text.
4. Replace -- file:/// with s3a://your.bucketname.
5. Name your new plugin, say s3-\<bucketname\>

You should now be able to talk to data stored on S3 using the S3a library.

## S3 Example

```
{
  "type": "file",
  "enabled": true,
  "connection": "s3a://apache.drill.cloud.bigdata/",
  "workspaces": {
    "root": {
      "location": "/",
      "writable": false,
      "defaultInputFormat": null
    },
    "tmp": {
      "location": "/tmp",
      "writable": true,
      "defaultInputFormat": null
    }
  },
  "formats": {
    "psv": {
      "type": "text",
      "extensions": [
        "tbl"
      ],
      "delimiter": "|"
    },
    "csv": {
      "type": "text",
      "extensions": [
        "csv"
      ],
      "delimiter": ","
    },
    ....
    
  }
}
```

