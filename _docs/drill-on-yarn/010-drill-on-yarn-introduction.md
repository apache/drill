---
title: "Drill-on-YARN Introduction"
date: 2018-03-18 20:11:39 UTC
parent: "Drill-on-YARN"
---  

As of Drill 1.13, Drill integrates with YARN to manage resources. Drill becomes a long running application with YARN. When you launch Drill, YARN automatically deploys (localizes) the Drill software onto each node, avoiding the need to pre-install Drill on each node. Resource management is simplified because YARN is aware of the resources dedicated to Drill.

Before you install and deploy Drill under YARN, you should be familiar with YARN concepts, such as the Resource Manager (RM), Node Manager (NM), and so on. You should also have a working Drill cluster that you want to launch under YARN. Drill configuration is best tested by launching Drill directly. You can launch Drill
under YARN when the configuration becomes stable. 

##YARN Resource Settings

Drill uses all available resources to run queries at optimal speed. When
running Drill under YARN, you inform YARN of the resources that Drill will consume. Drill does not limit itself to the YARN settings; instead the YARN settings inform YARN of the resources that Drill will consume so that YARN
does not over-allocate those same resources to other tasks. 

All YARN distributions provide settings for memory and CPU (called “vcores” by YARN). Some
distributions also provide disk settings. 

For memory, you first configure Drill’s memory as described below, then you inform YARN of the Drill configuration. 

Drill will use all available disk I/Os. Drill will also use all available CPU. Consider enabling Linux
cgroups to limit Drill's CPU usage to match the YARN vcores allocation.  

##Drill-on-YARN Components  

Drill-on-YARN uses the following components: 

- **Drill distribution archive:** The original .tar.gz file for your Drill distribution. DrillonYARN
uploads this archive to your distributed file system (DFS). YARN downloads it (localized
it) to each worker node.  
- **Drill site directory:** A directory that contains your Drill configuration and custom jar files.
DrillonYARN copies this directory to each worker node.  
- **Configuration:** A configuration file which tells DrillonYARN
how to manage your Drill cluster. This file is separate from your configuration files for Drill itself.
- **DrillonYARN client:** A command line program to start, stop and monitor your YARN-managed Drill cluster. 
- **Drill Application Master (AM):** The software that works with YARN to request resources, launch Drillbits, and so on. The AM provides a web UI to manage your Drill cluster.
- **Drillbit:** The Drill daemon software that YARN runs on each node.  

##Overview of Steps Required to Run Drill Under YARN
To launch Drill under YARN, you will complete the following key steps. Each step is explained in detail in the following sections of the Drill-on-YARN documentation. 

- Create a Drill site directory with your site-specific files.
- Configure Drill-on-YARN using the the drill-on-yarn.conf configuration file.
- Use the Drill-on-YARN client tool to launch your Drill cluster.
- Use the Drill-on-YARN client or web UI to monitor and shut down the Drill cluster.
