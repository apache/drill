---
title: "Drill-on-YARN Command-Line Tool"
date: 2018-11-02
parent: "Drill-on-YARN"
---  

Drill provides the drill-on-yarn command-line tool to start, stop, resize and check the status of your Drill cluster. The tool is located in:  

       $DRILL_HOME/bin/drill-on-yarn.sh site $DRILL_SITE command

Where command is one of those described below.  

##Start the Drill Cluster  

Start your drill cluster with the start command:  

       $DRILL_HOME/bin/drill-on-yarn.sh start  

The command shows the startup status followed by a summary of the application:  

       Launching DrillonYARN...
       Application ID: application_1462842354064_0001
       Application State: ACCEPTED
       Starting......
       Application State: RUNNING
       Tracking URL:
       http://10.250.50.31:8088/proxy/application_1462842354064_0001/
       Application Master URL: http://10.250.50.31:8048/


The first line confirms which cluster is starting by displaying the cluster name from your
configuration file. The next line shows YARN’s application ID and tracks the job status from
Accepted to Running. Once the job starts, you’ll see YARN’s job tracking URL along with
Drill-on-YARN’s web UI url. Use this URL to visit the web UI described below. Once the application starts, Drill-on-YARN writes an “appid” file into your master directory:  

       ls $MASTER_DIR
       …
       drillbits1.appid  

The file name is the same as your Drill cluster ID. The file contains the id if the Drill-on-YARN application for use by the other commands described below. You can run only one Drill AM at a time. If you attempt to start as second one from the same client machine on which you started the first, the client command will complain that the appid file already exists. If you attempt to start the cluster from a different node, then the second AM will detect the conflict and will shut down again.  

##Drill Cluster Status 

You can retrieve basic information about the Drill cluster as follows:

       $DRILL_HOME/bin/drill-on-yarn.sh status

You will see output something like the following:  

       Application ID: application_1462842354064_0001
       Application State: RUNNING
       Host: yosemite/10.250.50.31
       Tracking URL:
       http://10.250.50.31:8088/proxy/application_1462842354064_0001/
       Queue: default
       User: drilluser
       Start Time: 20160509
       16:56:40
       Application Name: DrillonYARN
       AM State: LIVE
       Target Drillbit Count: 1
       Live Drillbit Count: 1
       For more information, visit: http://10.250.50.31:8048/  


The first two several lines give you information about YARN’s state: the application ID, the
application state and YARN’s tracking URL for the application. Next is the host on which the Drill AM is running, the queue on which the application was placed and the user who submitted the application. The start time tells you when YARN started the application.
The next few lines are specific to Drill: the name of the application (which you configured in the Drill-on-YARN configuration file), the Drill application master URL, the number of Drillbits you requested to run and the number actually running. Finally, the last line gives you the URL to use to access the Drill-on-YARN web UI described below.  

##Stop the Drill Cluster
You can stop the Drill cluster from the command line:  

       $DRILL_HOME/bin/drill-on-yarn.sh stop  

Note that this command is “forceful”, it kills any in-flight queries. The output tracks the shutdown and displays the final YARN application status:  

       Stopping Application ID: application_1462842354064_0001
       Stopping...
       Stopped.
       Final status: SUCCEEDED  

##Resize the Drill Cluster
You can add or remove nodes to your Drill cluster while the cluster runs using the re-size
command. You can specify the change either by giving the number of nodes you want to run:  

       $DRILL_HOME/bin/drill-on-yarn.sh resize 10  

Or by specifying the change in node count: + for increase, for decrease. To add two nodes:  

       $DRILL_HOME/bin/drill-on-yarn.sh resize +2  

To remove three nodes:  

       $DRILL_HOME/bin/drill-on-yarn.sh resize 3  

Drill will add nodes only if additional nodes are available from YARN. If you request to stop more nodes than are running, Drill stops all the running nodes. Note that in the current version of Drill, stopping nodes is a forceful operation: any in-flight queries will fail.  

##Clean the DFS Files  

If you run Drill-on-YARN for a temporary cluster, Drill will leave the Drill software archive in your designated DFS directory. You can remove those files with the following:  

       $DRILL_HOME/bin/drill-on-yarn.sh clean  

Specifically, the first start uploads your Drill archive to DFS. Stop leaves the archive in DFS. Subsequent start commands reuse the cached archive if it is the same size as the version on the local disk. Clean removes the cached file, forcing Drill to upload a fresh copy if you again restart the Drill cluster.