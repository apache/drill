---
title: "Monitoring Metrics"
date: 2016-07-14 18:41:09 UTC
parent: "Performance Tuning"
--- 

As of Drill 1.7, Drill uses JMX ([Java Management Extensions](https://docs.oracle.com/javase/tutorial/jmx/)) to monitor queries at runtime. JMX provides the architecture to dynamically manage and monitor applications. JMX collects Drill system-level metrics that you can access through the Metrics tab in the Drill Web Console or a remote JMX monitoring tool, such as JConsole or the VisualVM + MBeans plugin. The Web Console Metrics tab contains the collected metrics as tables, counters, histograms, and gauges via JMX.  

## Remote Monitoring  
You can enable the remote JMX Java feature to monitor a specific JVM from a remote location. You can enable remote JMX with or without authentication. See the [Java documentation](http://docs.oracle.com/javase/7/docs/technotes/guides/management/agent.html). 

In `$DRILL_HOME/conf/drill-env.sh`, use the `DRILLBIT_JAVA_OPTS` variable to pass the relevant parameters. For example, to add remote monitoring on port 8048 without authentication:

       export DRILLBIT_JAVA_OPTS=”$DRILLBIT_JAVA_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=8048”  

## Disabling Drill Metrics  
JMX metric collection is enabled, by default. You can disable the metrics option if needed. 

In `$DRILL_HOME/conf/drill-env.sh`, set the `drill.metrics.jmx.enabled` option to false through the `DRILLBIT_JAVA_OPTS` variable. Add the variable in `drill-env.sh` if it does not exist:

       export DRILLBIT_JAVA_OPTS="$DRILLBIT_JAVA_OPTS -Ddrill.metrics.jmx.enabled=false"

The following table lists the predefined Drill system-level metrics that you can view through a JMX monitoring tool or the Drill Web Console:  

|    Metric                 | Description                                                                                                                                                         |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| drill.queries.running     | The   number of queries running for which this drillbit is the Foreman.                                                                                              |
| drill.queries.completed   | The   number of queries completed, canceled, or failed for which this drillbit was   the Foreman.                                                                    |
| drill.fragments.running   | The   number of query fragments currently running in the drillbit.                                                                                                  |
| drill.allocator.root.used | The   amount of memory (in bytes) used by the internal memory allocator.                                                                                            |
| drill.allocator.root.peak | The   peak amount of memory (in bytes) used by the internal memory allocator.                                                                                       |
| heap.used                 | The   amount of heap memory (in bytes) used by the JVM.                                                                                                             |
| non-heap.used             | The   amount of non-heap memory (in bytes) used by the JVM.                                                                                                         |
| count                     | The   number of live threads, including daemon and non-daemon threads.                                                                                              |
| fd.usage                  | The   ratio of used file descriptors to total file descriptors on *nix systems.                                                                                     |
|       direct.used         | The   amount of direct memory (in bytes) used by the JVM. This metric is useful for   debugging Drill issues.                                                       |
| runnable.count            | The   number of threads executing an action in the JVM. This metric is useful for   debugging Drill issues.                                                         |
| waiting.count             | The   number of threads waiting to execute. Typically, threads waiting on other   threads to perform an action. This metric is useful for debugging Drill   issues. |
| blocked.count             | The   number of threads that are blocked because they are waiting on a monitor   lock. This metric is useful for debugging Drill issues.                            |



