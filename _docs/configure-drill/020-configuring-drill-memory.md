---
title: "Configuring Drill Memory"
parent: "Configure Drill"
---

This section describes how to configure the amount of direct memory allocated to a Drillbit for query processing in any Drill cluster, multitenant or not. The default memory for a Drillbit is 8G, but Drill prefers 16G or more depending on the workload. The total amount of direct memory that a Drillbit allocates to query operations cannot exceed the limit set.

Drill uses Java direct memory and performs well when executing
operations in memory instead of storing the operations on disk. Drill does not
write to disk unless absolutely necessary, unlike MapReduce where everything
is written to disk during each phase of a job.

The JVM’s heap memory does not limit the amount of direct memory available in
a Drillbit. The on-heap memory for Drill is typically set at 4-8G (default is 4), which should
suffice because Drill avoids having data sit in heap memory.

## Modifying Drillbit Memory

You can modify memory for each Drillbit node in your cluster. To modify the
memory for a Drillbit, edit the `XX:MaxDirectMemorySize` parameter in the
Drillbit startup script, `drill-env.sh`, located in `<drill_installation_directory>/conf`.

{% include startnote.html %}If XX:MaxDirectMemorySize is not set, the limit depends on the amount of available system memory.{% include endnote.html %}

After you edit `<drill_installation_directory>/conf/drill-env.sh`, [restart the Drillbit]({{ site.baseurl }}/docs/starting-drill-in-distributed-mode) on the node.

## About the Drillbit startup script

The drill-env.sh file contains the following options:

    DRILL_MAX_DIRECT_MEMORY="8G"
    DRILL_MAX_HEAP="4G"

    export DRILL_JAVA_OPTS="-Xms1G -Xmx$DRILL_MAX_HEAP -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G -ea"

The DRILL_MAX_DIRECT_MEMORY is the Java direct memory. The DRILL_MAX_HEAP is the maximum theoretical heap limit for the JVM. Xmx specifies the maximum memory allocation pool for a Java Virtual Machine (JVM). Xms specifies the initial memory allocation pool.

