---
title: "Stopping Drill"
date: 2018-12-08
parent: Install Drill
---

You can abruptly stop the Drill process on a node, or you can gracefully shut down the Drill process on a node. When you stop the Drill process on a node, active queries cannot complete if they need additional time beyond the default five second wait period. In Drill 1.12 and later, you can use the `graceful_stop` command to transition a Drillbit into a quiescent state in which the Drillbit can complete in-progress queries before shutting down.  

###Stopping the Drill Process
To stop the Drill process on the node, issue the `drillbit.sh stop` command, as shown:  

       bin/drillbit.sh stop   

###Gracefully Shutting Down the Drill Process

Graceful Shutdown is enabled by default. You can gracefully shut down a Drillbit from the command line or the Drill Web UI. 

You can only use the Graceful Shutdown option in the Drill Web UI to shut down the Drillbit from which you accessed the Drill Web UI. For example, if you accessed the Drill Web UI at `http://drillbit1:8047`, you can only use the Graceful Shutdown option to shut down drillbit1. When you initiate graceful shutdown from the Drill Web UI, the console posts an alert stating that a graceful shutdown was triggered. You can see the progress of the shut down as the Drillbit completes queries and transitions through the quiescent state. 

**Note:** If security ([https]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/)) is enabled, only and administrator can perform a graceful shutdown.  

**How A Drillbit Shuts Down Gracefully**  

When a Drillbit shuts down gracefully, it transitions through a quiescent state to complete in-progress queries. 

The following list describes the various states that a Drillbit transitions through, including the quiescent state and phases within that state:  

- **Start**: The Drillbit is initializing. For example, when you issue the `drillbit.sh start` or `drillbit.sh restart` command.  
- **Online**: The Drillbit has started and registered with the cluster coordinator, such as ZooKeeper. ZooKeeper then shares the state of the Drillbit with other Drillbits in the cluster. Drillbits in the online state can all accept and process queries.  
- **Quiescent**: When a Drillbit receives a graceful shutdown request, the Drillbit transitions into the quiescent state and shares its status change with the ZooKeeper. The ZooKeeper notifies the other Drillbits in the cluster of the Drillbit’s status change. Once the other Drillbits get the status update, they do not assign work to the Drillbit. However, if the Foreman assigns work to the Drillbit as the status update occurs, the Drillbit waits to complete work before shutting down. The quiescent state has three phases: grace, draining, and offline.  
       - **Grace**: A period in which a Drillbit can accept new queries from the Foreman. You can configure (at the system level) the amount of time a Drillbit can remain in this phase using the `drill.exe.grace_period_ms` option. Set this value in milliseconds. The default value is 0. There is no maximum limit. Ideally, you should give as little time as possible or no longer than twice the default heartbeat time of the ZooKeeper. For example, if the heartbeat is 5 seconds, set the value to the equivalent of 10 seconds in milliseconds (10000).   
       - **Draining**:  When the grace period ends, the Drillbit enters the draining phase of the quiescent state. In this phase, the Drillbit cannot accept new queries, but waits for the running queries to complete. You can view the draining queries in the Drill Web UI. The Index page in the Web UI shows the queries and fragments currently running on the node.  
       - **Offline**: The phase the Drillbit enters after completing all queries.   
- **Shutdown**: The final state in which a Drillbit removes itself from the ZooKeeper registry.  

**Shutting Down a Drillbit Gracefully**  
You can gracefully shut down a Drillbit from the command line or the Drill Web UI. 
 
From the command line, run the following command on the node you want to shut down:  

       drillbit.sh graceful_stop

From the Drill Web UI, enter the following URL in your browser’s address bar:

       http://<IP address or host name>:8047 or https://<IP address or host name>:8047

In the Drill Web UI, open the Index page, and click Shutdown next to the Drillbit you want to shut down.
  





