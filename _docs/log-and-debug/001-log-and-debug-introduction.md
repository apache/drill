---
title: "Log and Debug Introduction"
parent: "Log and Debug"
---

You can use the Drill logs in conjunction with query profiles to troubleshoot issues that you encounter. Drill uses Logback as its default logging system. Logback behavior is defined by configurations set in <drill_installation_directory>/conf/logback.xml. 

You can configure Logback to enable specific loggers for particular components. You can also enable Logback to output log messages to Lilith, a desktop application that you can use for socket logging. By default, Drill outputs log files to /var/log/drill.

Drill provides two standard output files:  

* drillbit.out
* drill.log

Drill also provides a special file, drillbit_queries.json, on each Drill node. This log provides the QueryID and profile for every query run on a Drillbit. The Profile view in the Drill Web UI lists the last one-hundred queries that Drill ran. To see information for queries beyond the last one-hundred, you can view the drillbit_queries.json file on each Drill node.

Drill also provides [audit logging]({{site.baseurl}}/docs/query-audit-logging/) of queries executed by various drillbits in the cluster. 
