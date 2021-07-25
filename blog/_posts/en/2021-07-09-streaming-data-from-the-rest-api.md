---
layout: post
title: "Streaming data from the Drill REST API"
code: streaming-data-from-the-drill-rest-api
excerpt: The release of Apache Drill 1.19 saw a major change under the hood of Drill's REST API with the introduction of a streaming data path for query results moving from Drill and over the network to the initiating client.  The result is better memory utilisation, less blocking and a more reliable API.

authors: ["jturton"]
---

Anyone who's used a UNIX pipe, or even just watched something on Netflix, is at least a little familiar with the idea of processing data in a streaming fashion.  While your data are small in size compared to available memory and I/O speeds, streaming is something you can afford to dispense with.  But when you cannot fit an entire dataset in RAM, or when you have to download an entire 4K movie before you can start playing it, then streaming data processing can make a game changing difference.

With the release of version 1.19, Drill will stream JSON query result data over an HTTP response to the client that initiated the query using the REST API.  And if anything can easily get big compared to your available RAM or network speed, it's query results coming back from Drill.  It's important to note here that JSON over HTTP is never going to be the most _efficient_ way to move big data around[^1].  JDBC, ODBC and [innovations around them](https://uwekorn.com/2019/11/17/fast-jdbc-access-in-python-using-pyarrow-jvm.html) will always win a speed race over JSON/HTTP, and simply network mounting or copying Parquet files out of your big data storage pool (be that HDFS, S3, or something else) is probably going to beat everything else you try once you've got really big data volumes.

Where JSON and HTTP _do_ win is universality: today it's hard to imagine a client hardware and software stack that doesn't provide JSON and HTTP out of the box with minimal effort.  So it's important that they work as well as is possible, in spite of the alternatives that exist.  The new streaming query results delivery on the server side means that Drill's heap memory isn't pressurised by having to buffer entire result sets before it starts to transmit them over the network.  Even existing REST API clients that do no stream processing of their own will benefit from better reliability because of this.

To fully realise the benefits of streaming query result data, clients can _themselves_ operate on the HTTP response they receive in a streaming fashion, thereby potentially starting to process records before Drill has even finished materialising them and avoiding holding the full set in memory if they choose.  At the transport level (when there are enough results to warrant it), the HTTP response headers from the Drill REST API will include a `Transfer-Encoding: chunked` header indicating that data transmission in chunks will ensue.  Most HTTP client libraries will abstract this implementation detail, instead presenting you with a stream which you can provide as the input to a streaming JSON parser.

If you set out to develop a streaming HTTP client for the Drill REST API, do take note that the schema of the JSON query result is _not_ a [streaming JSON format](https://en.wikipedia.org/wiki/JSON_streaming) like "JSON lines".  This means that you must be careful about which JSON objects you parse entirely in a single call, particularly avoiding any parent of the `rows` property in the query result which would see you parse essentially the entire payload in a single step.  The newly released version 1.1.0 of the Python [sqlalchemy-drill](https://pypi.org/project/sqlalchemy-drill/) library includes [an implementation of a streaming HTTP client](https://github.com/JohnOmernik/sqlalchemy-drill/blob/master/sqlalchemy_drill/drilldbapi/_drilldbapi.py) based on the [ijson](https://pypi.org/project/ijson/) streaming JSON parser which might make for a useful reference.

In closing, and for a bit of fun, here's the log from a short IPython session where I use sqlalchemy-drill to run `SELECT *` on a remote 17 billion record table over a 0.5 Mbit/s link and start scanning through (a steady trickle of) rows in seconds.

```ipython
In [4]: r = engine.execute('select count(*) from dfs.ws.big_table')
INFO:drilldbapi:received Drill query ID 1f211888-cc20-fe6f-69d1-6584c5caa2df.
INFO:drilldbapi:opened a row data stream of 1 columns.

In [5]: next(r)
Out[5]: (17437571247,)

In [6]: r = engine.execute('select * from dfs.ws.big_table')
INFO:drilldbapi:received Drill query ID 1f211838-73df-1506-a74e-f5695f6b0ff5.
INFO:drilldbapi:opened a row data stream of 21 columns.

In [7]: while True:
   ...:     _ = next(r)
      ...:
      INFO:drilldbapi:streamed 10000 rows.
      INFO:drilldbapi:streamed 20000 rows.
      INFO:drilldbapi:streamed 30000 rows.
      INFO:drilldbapi:streamed 40000 rows.
```

[^1]: Some mitigation is possible.  JSON representations of tabular big data are typically extremely compressible and you can reduce the bytes sent over the network by 10-20x by enabling HTTP response compression on a web server like Apache placed in front of the Drill REST API.

