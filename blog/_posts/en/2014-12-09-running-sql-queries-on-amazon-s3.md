---
layout: post
title: "Running SQL Queries on Amazon S3"
code: running-sql-queries-on-amazon-s3
excerpt: Drill enables you to run SQL queries directly on data in S3. There's no need to ingest the data into a managed cluster or transform the data. This is a step-by-step tutorial on how to use Drill with S3.
date: 2018-02-09 00:16:07 UTC
authors: ["namato"]
---
The functionality and sheer usefulness of Drill is growing fast.  If you're a user of some of the popular BI tools out there like Tableau or SAP Lumira, now is a good time to take a look at how Drill can make your life easier, especially if  you're faced with the task of quickly getting a handle on large sets of unstructured data.  With schema generated on the fly, you can save a lot of time and headaches by running SQL queries on the data where it rests without knowing much about columns or formats.  There's even more good news:  Drill also works with data stored in the cloud.  With a few simple steps, you can configure the S3 storage plugin for Drill and be off to the races running queries.  In this post we'll look at how to configure Drill to access data stored in an S3 bucket.

If you're more of a visual person, you can skip this article entirely and [go straight to a video](https://www.youtube.com/watch?v=w8gZ2nn_ZUQ) I put together that walks through an end-to-end example with Tableau.  This example is easily extended to other BI tools, as the steps are identical on the Drill side.

At a high level, configuring Drill to access S3 bucket data is accomplished with the following steps on each node running a drillbit.

* Download and install the [JetS3t](http://www.jets3t.org/) JAR files and enable them.
* Add your S3 credentials in the relevant XML configuration file.
* Configure and enable the S3 storage plugin through the Drill web interface.
* Connect your BI tool of choice and query away.

Consult the [Architectural Overview](https://cwiki.apache.org/confluence/display/DRILL/Architectural+Overview) for a refresher on the architecture of Drill.

## Prerequisites

These steps assume you have a [typical Drill cluster and ZooKeeper quorum](https://cwiki.apache.org/confluence/display/DRILL/Apache+Drill+in+10+Minutes) configured and running.  To access data in S3, you will need an S3 bucket configured and have the required Amazon security credentials in your possession.  An [Amazon blog post](http://blogs.aws.amazon.com/security/post/Tx1R9KDN9ISZ0HF/Where-s-my-secret-access-key) has more information on how to get these from your account.

## Configuration Steps

To connect Drill to S3, all of the drillbit nodes will need to access code in the JetS3t library developed by Amazon.  As of this writing, 0.9.2 is the latest version but you might want to check [the main page](https://jets3t.s3.amazonaws.com/toolkit/toolkit.html) to see if anything has been updated.  Be sure to get version 0.9.2 or later as earlier versions have a bug relating to reading Parquet data.

```bash
wget http://bitbucket.org/jmurty/jets3t/downloads/jets3t-0.9.2.zip
cp jets3t-0.9.2/jars/jets3t-0.9.2.jar $DRILL_HOME/jars/3rdparty
```

Next, enable the plugin by editing the file:

```bash
$DRILL_HOME/bin/hadoop_excludes.txt
```

and removing the line `jets3t`.

Drill will need to know your S3 credentials in order to access data there. These credentials will need to be placed in the core-site.xml file for your installation.  If you already have a core-site.xml file configured for your environment, add the following parameters to it, otherwise create the file from scratch.  If you do end up creating it from scratch you will need to wrap these parameters with `<configuration>` and `</configuration>`.

```xml
<property>
  <name>fs.s3.awsAccessKeyId</name>
  <value>ID</value>
</property>

<property>
  <name>fs.s3.awsSecretAccessKey</name>
  <value>SECRET</value>
</property>

<property>
  <name>fs.s3n.awsAccessKeyId</name>
  <value>ID</value>
</property>

<property>
  <name>fs.s3n.awsSecretAccessKey</name>
  <value>SECRET</value>
</property>
```

The steps so far give Drill enough information to connect to the S3 service.  Remember, you have to do this on all the nodes running drillbit.

Next, let's go into the Drill web interface and enable the S3 storage plugin.  In this case you only need to connect to **one** of the nodes because Drill's configuration is synchronized across the cluster.  Complete the following steps:

1. Point your browser to `http://<host>:8047`
2. Select the 'Storage' tab.
2. A good starting configuration for S3 can be entirely the same as the `dfs` plugin, except the connection parameter is changed to `s3n://bucket`.  So first select the `Update` button for `dfs`, then select the text area and copy it into the clipboard (on Windows, ctrl-A, ctrl-C works).
2. Press `Back`, then create a new plugin by typing the name into the `New Storage Plugin`, then press `Create`.  You can choose any name, but a good convention is to use `s3-<bucketname>` so you can easily identify it later.
3. In the configuration area, paste the configuration you just grabbed from 'dfs'.  Change the line `connection: "file:///"` to `connection: "s3n://<bucket>"`.
4. Click `Update`.  You should see a message that indicates success.
 
Note: Make sure the URI has scheme "s3n", not "s3". It will not work with "s3".

At this point you can run queries on the data directly and you have a couple of options on how you want to access it.  You can use Drill Explorer and create a custom view (based on an SQL query) that you can then access in Tableau or other BI tools, or just use Drill directly from within the tool.

You may want to check out the [Tableau demo](http://www.youtube.com/watch?v=jNUsprJNQUg).

With just a few lines of configuration, you've just opened the vast world of data available in the Amazon cloud and reduced the amount of work you have to do in advance to access data stored there with SQL.  There are even some [public datasets](https://aws.amazon.com/datasets) available directly on S3 that are great for experimentation.

Happy Drilling!
