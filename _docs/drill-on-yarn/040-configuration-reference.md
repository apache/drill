---
title: "Configuration Reference"
date:  
parent: "Drill-on-YARN"
---  

The Creating a Basic Drill Cluster topic presented the minimum configuration needed to launch Drill under YARN. Additional configuration options are available for specialized cases. Refer to the drill-on-yarn-example.conf for information about the other options.  

##Application Name  
The application name appears when starting or stopping the Drill cluster and in the
Drill-on-YARN web UI. Choose a name helpful to you:  

       app-name: "My Drill Cluster"  

##Drill Distribution Archive
The Drill distribution archive is assumed to expand to create a folder that has the same name as the archive itself (minus the .tar.gz suffix). That is, the archive apache-drill-x.y.z.tar.gz is assumed to expand to a directory named apache-drill-x.y.z. Apache Drill archives follow this pattern. In specialized cases, you may have to create your own archive. If you do, it is most convenient if you follow the same pattern. However, if cannot follow the pattern, you can configure Drill-on-YARN to follow a custom pattern using the drill-install.dir-name option:  

       drill-install:{
              clientpath: "/path/to/ your-custom-archive.tar.gz"
              dirname: "your-drill-directory"
         }  

Where:  

`/path/to/ your-custom-archive.tar.gz` is the location of your archive. `your-drill-directory`
is the name of your Drill directory within the archive.  

##Customize Web UI Port
If you run multiple Drill clusters per YARN cluster, then YARN may choose to place two Drill AM
processes on the same node. To avoid port conflicts, change the HTTP port for one or both of
the Drill clusters:  

       drill.yarn:{
              http: {
               port: 12345
              }
       }  

##Customize Application Master Settings
The following settings apply to the Application Master. All are prefixed with `drill.yarn.am.`  

| Name            | Description                                                        | Default |
|-----------------|--------------------------------------------------------------------|---------|
| memorymb        | Memory, in MB, to allocate to the AM.                              | 512     |
| vcores          | Number of CPUS to allocate to the AM.                              | 1       |
| heap            | Java heap for the AM.                                              |  450M   |
| node-label-expr | YARN node label expression to use to select nodes to run the   AM. | None    |
  

##Drillbit Customization  

The following Drill-on-YARN configuration options control the Drillbit processes. All properties
start with drill.yarn.drillbit.  

| Name              | Description                                                                                       |  Default |
|-------------------|---------------------------------------------------------------------------------------------------|----------|
| memory-mb         | Memory, in MB, to allocate to the Drillbit.                                                       | 13000    |
| vcores            | Number of CPUS to allocatet to the AM.                                                            | 4        |
| disks             | Number of disk equivalents consumed by Drill (on versions of   YARN that support disk resources.) | 1        |
| heap              | Java heap memory.                                                                                 | 4G       |
| max-direct-memory | Direct (offheap) memory for the Drillbit.                                                         | 8G       |
| log-gc            | Enables Java garbage collector logging.                                                           | FALSE    |
| class-path        | Additional classpath entries.                                                                     | blank    |  

Note that the Drillbit node expression is set in the labeled pool below.  

##Cluster Groups
YARN was originally designed for MapReduce jobs that can run on any node, and that often can be combined onto a single node. Compared to the traditional MapReduce jobs, Drill has additional constraints:  

- Only one Drillbit (per Drill cluster) can run per host (to avoid port conflict.)
- Drillbits work best when launched on the same host as the data that the Drillbit is to scan.


###Basic Cluster
A basic cluster launches n Drillbits on distinct nodes anywhere in your YARN cluster. The basic cluster is great for testing and other informal tasks: just configure the desired vcores and memory, along with a number of nodes, then launch Drill. YARN will locate a set of suitable hosts anywhere on the YARN cluster.  

###Labeled Hosts
Drill-on-YARN can handle node placement directly without the use of labeled queues. You use the “labeled” pool type. Then, set the drillbit-label-expr property to a YARN label expression that matches the nodes on which Drill should run. You will most often care only about Drillbit placement. Finally, indicate the number of Drillbits to run on the selected nodes. 

###Named Hosts
You can configure Drill-on-YARN to run on a specific set of hosts. However, you must keep the list synchronized with your YARN cluster. If you list a host that is not available to YARN, then Drill cannot start a Drillbit on that host. Also, if the host does
not have sufficient resources available, the Drillbit will not run.


