---
title: "Multiple Drill Clusters"
date: 2018-11-02
parent: "Drill-on-YARN"
---  

Drill-on-YARN allows you to easily define multiple Drill clusters on a single YARN cluster. Each Drill cluster is a collection of Drillbits that work as an independent unit. For example, you might define one test cluster of a few machines on the same physical cluster that runs larger clusters for, say, Development and Marketing.  

Drill clusters coordinate using ZooKeeper, so you must assign each cluster a distinct ZooKeeper
entry. YARN may launch Drillbits from different clusters on the same physical node, so each
Drill cluster must use a distinct set of ports. Since each cluster requires its own setup, you must create a separate site directory for each. The instructions below explain the required setup.  

##Create a New Site Directory
Create a new site directory for your new cluster. Let’s say that your new cluster has the name
“second”. Using the same structure as before, create a new site directory under your master
directory:  

       export SECOND_SITE=$MASTER_DIR/second
       mkdir $SECOND_SITE  

Copy files into this new site directory as you did to create the first one. You can copy and modify an existing set of files, or create the site from scratch.

At a minimum, you must set the following configuration options in drill-override.conf:  

       drill.exec: {
               cluster-id: "drillbits",
               zk: {
                  root: "second"
                  connect: "zkhost: 2181"
               }
              rpc {
                  user.server.port: 41010
                  bit.server.port: 41011
              }
                  http.port: 9047
       }  

You have two options for how your new cluster interacts with the existing cluster. The normal
case is a shared-nothing scenario in which the two clusters are entirely independent at the
configuration level. For this case, ensure that the zk.root name is distinct from any existing
cluster.

In the more advanced case, if both clusters share the same zk.root value, then they will
share settings such as storage plugins. If the clusters share the same root, then they must have distinct cluster-id values.  

In addition, the three ports must have values distinct from all other clusters. In the example
above, we’ve added a 1 to the first digit of the default port numbers; you can choose any available ports.  

##Drill-on-YARN Configuration
Create the drill-on-yarn.conf file as described before. The following must be distinct
for your cluster:  

       drill.yarn: {
              app-name: "Second Cluster"
                     dfs: {
                          app-dir: "/user/drill2"
                     }
              http : {
                     port: 9048
                }
       }

That is, give your cluster a distinct name, a distinct upload directory in DFS, and a distinct port number.  

##Start the Cluster
Use the site directory for the second cluster to start the cluster:  

       $DRILL_HOME/bin/drill-on-yarn.sh site
       $SECOND_SITE start  

