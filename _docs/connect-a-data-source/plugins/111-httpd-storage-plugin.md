---
title: "HTTPD Storage Plugin"
date: 2017-03-15 19:34:55 UTC
parent: "Configure Drill"
---

As of version 1.9, Drill can natively ingest and query web server logs. To configure Drill to read server logs, you must modify the extensions section in the dfs storage plugin configuration, as shown below:

    "httpd": {
      "type": "httpd",
      "logFormat": "%h %t \"%r\" %>s %b \"%{Referer}i\" \"%{user-agent}i\"",
      "timestampFormat": null
    }  

{% include startnote.html %}The logFormat section must match the format of the log files, otherwise Drill cannot correctly parse the logs.{% include endnote.html %}

## HTTPD Format Strings  
The following table lists the fields that log files can include. The `timestampformat` is optional, but you can include a format for the time stamp and Drill will parse the times in the log files into Drill dates.


|Format String | Variable Name |
|--------------|---------------|
|%a	| connection.client.ip |
|%{c}a | connection.client.peerip |
|%A	| connection.server.ip |
|%B	| response.body.bytes	|
|%b	| response.body.bytesclf |
|%{Foobar}C	 | request.cookies.* |
|%D	| server.process.time	|
|%{Foobar}e	| server.environment.* |
|%f	 | server.filename	|
|%h	| connection.client.host |
|%H | request.protocol | 
|%{Foobar}i | request.header.	|
|%k | connection.keepalivecount	|
|%l | connection.client.logname	|
|%L | request.errorlogid	STRING
|%m | request.method	|
|%{Foobar}n | server.module_note.*	|
|%{Foobar}o | response.header.*	|
|%p | request.server.port.canonical	|
|%{canonical}p | connection.server.port.canonical	|
|%{local}p | connection.server.port	|
|%{remote}p | connection.client.port	|
|%P | connection.server.child.processid	|
|%{pid}P | connection.server.child.processid	|
|%{tid}P | connection.server.child.threadid	|
|%{hextid}P	| connection.server.child.hexthreadid	|
|%q	| request.querystring	|
|%r	| request.firstline	|
|%R	| request.handler	|
|%s	| request.status.original	|
|%>s | request.status.last	|
|%t | request.receive.time	|
|%{msec}t | request.receive.time.begin.msec	|
|%{begin:msec}t | request.receive.time.begin.msec	|
|%{end:msec}t | request.receive.time.end.msec	|
|%{usec}t | request.receive.time.begin.usec	|
|%{begin:usec}t | request.receive.time.begin.usec	|
|%{end:usec}t | request.receive.time.end.usec	|
|%{msec_frac}t | request.receive.time.begin.msec_frac	|
|%{begin:msec_frac}t | request.receive.time.begin.msec_frac	TIME.EPOCH
|%{end:msec_frac}t | request.receive.time.end.msec_frac	|
|%{usec_frac}t |	request.receive.time.begin.usec_frac	|
|%{begin:usec_frac}t |	request.receive.time.begin.usec_frac	|
|%{end:usec_frac}t | request.receive.time.end.usec_frac	|
|%T	| response.server.processing.time	|
|%u	| connection.client.user	|
|%U	| request.urlpath	|
|%v	| connection.server.name.canonical	|
|%V	| connection.server.name	|
|%X	| response.connection.status	|
|%I	| request.bytes	|
|%O	| response.bytes	|
|%{cookie}i	| request.cookies	|
|%{set-cookie}o	| response.cookies | 
|%{user-agent}i	| request.user-agent |
|%{referer}i | request.referer	|

## Additional Functionality
In addition to reading raw log files, the following functions are also useful when analyzing log files:  

* `parse_url(<url>)`:  This function accepts a URL as an argument and returns a map of the URL's protocol, authority, host, and path.
* `parse_query( <query_string> )`:  This function accepts a query string and returns a key/value pairing of the variables submitted in the request.

A function that parses User Agent strings and returns a map of all the pertinent information is available at: https://github.com/cgivre/drill-useragent-function
