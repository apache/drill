---
title: "Using the Drill-on-YARN Web UI"
date:  
parent: "Drill-on-YARN"
---  

Applications that run under YARN provide an Application Master (AM) process to manage the
application’s task. Drill provides the Drill-on-YARN Application Master for this purpose. When
you launch Drill using the command line tool, the tool asks YARN to launch Drill’s AM, which in
turn launches your Drillbits.  

The Drill application master provides a web UI to monitor cluster status and to perform simple
operations such as increasing or decreasing cluster size, or stop the cluster. You can reach the UI using the URL provided when the application starts. You can also follow the link from the YARN Resource Manager UI. Find the page for the Drill application. Click on
the Tracking URL link.  

The UI provides five pages:  
1. A main page that provides overall cluster status.  
2. A configuration page where you view the complete set of configuration variables which
the Drill AM uses.  
3. Detailed list of the running Drillbits.  
4. Management page with a number of simple operations to resize or stop the cluster.  
5. A history of stopped, killed or failed Drillbits. Use this to diagnose problems.  

##Main Page
The main page shows the state of the Drill cluster. 

Drill Cluster Status : the state of the Drill cluster, one of the following:  

- LIVE: Normal state: shows that your Drill cluster is running.
- ENDING: The cluster is in the process of shutting down.  


There is no “ENDED” state: once the cluster is shut down, the AM itself exists and the web UI is no longer available.  



- **Target Drillbit Count:** The number of Drillbits to run in the cluster. The actual number may be less if Drillbits have not yet started, or if YARN cannot allocate enough containers.
- **Live Drillbit Count:** Number of Drillbits that are ready for use. These have successfully started, have registered with ZooKeeper, and are ready for use. You can see the detail of all Drillbits (including those in the process of starting or stopping) using the Drillbits page. Each Drillbit must run on a separate node, so this is also the number of nodes in the cluster running Drill.  
- **Total Drillbit Memory and Virtual Cores:** The total number of YARN resources currently
allocated to running Drillbits.  
- **YARN Node Count, Memory and Virtual Cores :** Reports general information about YARN
itself including the number of nodes, the total cluster memory and total number of virtual cores.  
- **Groups:** Lists the cluster groups defined in the configuration file (of which only one is currently supported), along with the target and actual number of Drillbits in that group.

##Configuration Page
The configuration page shows the complete set of configuration values used for the current run.
The values come from your own configuration along with Drill-provided defaults. Use this page
to diagnose configuration-related issues. Names are shown in fully-expanded form. That is the
name “drill.yarn.http.port” refers to the parameter defined as follows in your configuration file:  

       drill.yarn:{
               http: {
                   port: 8048
              }
       }  

##Drillbits Page
The Drillbits page lists all drillbits in all states.



- **ID:** A sequential number assigned to each new Drillbit. Numbers may not start with 1 if you have previously shut down some Drillbits.  
- **Group:** The cluster group that started the Drillbit. (Cluster groups are from the configuration file.)  
- **Host:** The host name or IP address on which the Drillbit runs. If the Drillbit is in normal operating state, this field is also a hyperlink to the Web UI for the Drillbit.  
- **State:** The operating state of the Drillbit. The normal state is “Running.” The drillbit passes through a number of states as YARN allocates a container and launches a process, as the AM waits for the Drillbit to become registered in ZooKeeper, and so on. Similarly, the Drillbit passes through a different set of states during shutdown. Use this value to diagnose problems.
If the Drillbit is in a live state, then this field shows an “[X]” link that you can use to kill this particular Drillbit. Use this if the Drillbit has startup problems or seems unresponsive. During the shutdown process, the kill link disappears and is replaced with a “(Cancelled)” note.  
- **ZK State:** The ZooKeeper handshake state. Normal state is “START_ACK”, meaning that the
Drillbit has registered with ZooKeeper. This state is useful when diagnosing problems.   
- **Container ID:** The YARNassigned container ID for the Drillbit task. The ID is a link, it takes you to the YARN Node Manager UI for the Drillbit task.  
- **Memory and Virtual Cores:** The amount of resources actually allocated to the Drillbit by YARN.  
- **Start Time:** The date and time (in your local timezone, displayed in ISO format) when the
Drillbit launch started. This page will also display un-managed Drillbits, if present. An un-manage Drillbit is one that is running, has registered with ZooKeeper, but was not started by the Drill Application Master. Likely the Drillbit was launched using the drillbit.sh script directly. Use the host name to locate the machine running the Drillbit if you want to convert that Drillbit to run under YARN.  

##Manage Page
The Manage page allows you to re-size or stop the cluster. You can re-size the cluster by adding Drillbits, removing Drillbits or setting the cluster to a desired size. 

Drill is a long-running application. In normal practice, you leave Drill running indefinitely. You would shut down your Drill cluster only to, say, perform an upgrade of the Drill software or to change configuration options. When you terminate your Drill cluster, any in-progress queries will fail. Therefore, a good practice is to perform the shut down with users so that Drill is not processing any queries at the time of the shutdown.

When removing or shutting-down the cluster, you will receive a confirmation page asking if you
really do want to stop Drillbit processes. Click Confirm to continue.  

##History Page
The History page lists all Drillbits which have failed, been killed, or been restarted. The History page allows you to detect failures and diagnose problems. Use the YARN container ID listed on this page to locate the log files for the Drillbit.