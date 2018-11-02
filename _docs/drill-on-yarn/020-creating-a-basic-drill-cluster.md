---
title: "Creating a Basic Drill Cluster"
date: 2018-11-02
parent: "Drill-on-YARN"
---  

This topic walks you through the steps needed to create a basic Drill cluster.  

##The Client Machine  

YARN works by launching an application using a “client” application. For Drill, this is the
Drill-on-YARN client. The client can run on any machine that has both the Drill and Hadoop
software. Any host from which you currently launch YARN jobs can be the client. The client is not required to be part of the YARN cluster. 

When running Drill outside of YARN, you must install Drill on every node in the cluster. With YARN, you only need install Drill only on the client machine; Drill-on-YARN automatically deploys (“localizes”) Drill to the worker nodes.

When running Drill without YARN, many users place their configuration files and custom code
within the Drill distribution directory. When running under YARN, all your configuration and
custom code resides in the site directory; do not change anything in the Drill install.
(This allows Drill-on-YARN to upload your original Drill install archive without rebuilding it.)

Complete the following steps on the client machine:  

###Create a Master Directory  

To localize Drill files, the client tool requires a copy of the original Drill distribution archive and the location of your site directory. Assume all these components reside in a single “master directory” described as $MASTER_DIR . On the client machine, create the master directory, as shown:  

       export MASTER_DIR=/path/to/master/dir
       mkdir $MASTER_DIR
       cd $MASTER_DIR  

To build the master directory, you will: 
- Download the Drill archive to $MASTER_DRILL.
- Unpack the archive to create $DRILL_HOME.
- Create the site directory with the required configuration files.

The following steps provide the instructions for building the master directory:  

###Install Drill
These instructions assume you are installing Drill as part of the Drill-on-YARN
setup. You can use your existing Drill 1.8 or later install as long as it meets the required criteria. 

Follow the Drill [install directions]({{site.baseurl}}/docs/install-drill-introduction/) to install Drill on your client host. The install steps are different for YARN than for the Embedded or Cluster install. 

1. Select a Drill version. The name is used in multiple places below. For convenience, define an environment variable for the name:  
 
              export DRILL_NAME=apachedrillx.y.z
Replace x.y.z with the selected version.
2. Download the Drill version.  

              wget \ http://apache.mesi.com.ar/drill/drillx.y.z/$DRILL_NAME.tar.gz
Or use  

              curl o $DRILL_NAME.tar.gz \http://apache.mesi.com.ar/drill/drillx.y.z/$DRILL_NAME.tar.gz
Again, replace x.y.z with the selected version.

3. Expand the Drill distribution into this folder to create the master directory  

              tar -xzf $DRILL_NAME.tar.gz
4. For ease of following the remaining steps, call your expanded Drill folder $DRILL_HOME :

              export DRILL_HOME=$MASTER_DIR/$DRILL_NAME

Your master directory should now contain the original Drill archive along with an expanded copy
of that archive.  

###Create the Site Directory  

The site directory contains your site-specific files for Drill. If you are converting an existing Drill install, see the “Site Directory” section.  

Create the site directory within your master directory:

              export DRILL_SITE=$MASTER_DIR/site
              mkdir $DRILL_SITE

When you do a fresh install, Drill includes a conf directory under $DRILL_HOME. Use the files
in that directory to create your site directory.  

              cp $DRILL_HOME/conf/drilloverrideexample.conf \
              $DRILL_SITE/drilloverride.conf
              cp $DRILL_HOME/conf/drill-on-yarnexample.conf \
              $DRILL_SITE/drill-on-yarn.conf
              cp $DRILL_HOME/conf/drillenv.sh $DRILL_SITE  

Edit the above configuration files as per the Drill install instructions, and the Drill-on-YARN
instructions below. (Note that, under YARN, you set the Drill memory limits in
drill-on-yarn.sh instead of drillenv.sh.)

If you develop custom code (data sources or user-defined functions (UDFs)), place the Java JAR
files in $DRILL_SITE/jars. 

Your master directory should now contain the Drill software and your site directory with default files. You will use the site directory each time you start Drill by using the --site
(or --config) option. The following are examples, do not run these yet:

              drillbit.sh --site $DRILL_SITE
              drill-on-yarn.sh --site $DRILL_SITE

Once you have created your site directory, upgrades are trivial. Simply delete the old Drill
distribution and install the new one. Your files remain unchanged in the site directory.  

###Configure Drill-on-YARN using Existing Settings

The next step is to configure Drill. If you have used Drill, start with Drill
in distributed mode to learn which configuration options you need. YARN is an awkward
environment in which to learn Drill configuration. These instructions assume that you have already worked out the required configuration on a separate Drill install. Let's call that location $PROD_DRILL_HOME.

From $PROD_DRILL_HOME, copy the following to corresponding locations in $DRILL_SITE:  

              cp $PROD_DRILL_HOME/conf/drilloverride.conf $DRILL_SITE
              cp $PROD_DRILL_HOME/conf/drillenv.sh $DRILL_SITE
              cp $PROD_DRILL_HOME/jars/3rdparty/ yourJarName .jar $DRILL_SITE/jars

###Create Your Cluster Configuration File
The next step is to specify additional configuration which Drill-on-YARN requires to launch your Drill cluster. 

Start by editing $DRILL_SITE/drill-on-yarn.conf using your favorite editor. This file is
in the same HOCON format used by drill-override.conf.

Consult $DRILL_HOME/conf/drill-on-yarn-example.conf as an example. However,
do not just copy the example file; instead, copy only the specific configuration settings that you need; the others will automatically take the Drill-defined
default values.  

The following sections discuss each configuration option that you must set.  

###Drill Resource Configuration
The two key Drill memory parameters are Java heap size and direct memory. In a non-YARN
cluster, you set these in $DRILL_HOME/conf/drillenv.sh as follows (shown with the
default values):

              DRILL_MAX_DIRECT_MEMORY="8G"
              DRILL_HEAP="4G"  

Drill-on-YARN uses a different mechanism to set these values. You set the values in
drill-on-yarn.conf , then Drill-on-YARN copies the values into the environment variables
when launching each Drillbit.  

              drillbit: {
                    heap: "4G"
                    max-direct-memory: "8G"
                  }  

To create the Drill-on-YARN setup, simply copy the values directly from your pre-YARN
drillenv.sh file into the above configuration. (Drill-on-YARN copies the values back into
the environment variables when launching Drill.) 

Next, determine the container size needed to run Drill under YARN. Typically this size
is simply the sum of the heap and direct memory. However, if you are using custom libraries that perform their own memory allocation, or launch sub-processes, you must account for that
memory usage as well. The YARN memory is expressed in MB. For example, for the default
settings above, we need 12G of memory or 12288MB:  

              drillbit: {
                     memory-mb: 6144
                 }  

Finally, you must determine how much CPU to grant to Drill. Drill is a CPU intensive
operation and greatly benefits from each additional core. However, you can limit Drill’s CPU usage under YARN by specifying the number of YARN virtual cores (vcores) to allocate to Drill:  

              drillbit: {
                     vcores: 4
              }  

Note that in the above, each configuration setting was shown separately. In your actual file,
however, they appear within a single group as follows:  

              drillbit: {
                     heap: "4G"
                     max-direct-memory: "8G"
                     memory-mb: 6144
                     vcores: 4
                   }  


###Drillbit Cluster Configuration
Drill-on-YARN uses the concept of a “cluster group” of Drillbits to describe the set of Drillbits to launch. A group can be one of three kinds:  



- Basic: launches drillbits anywhere in the YARN cluster where a container is available.
- Labeled: Uses YARN labels to identify the set of nodes that should run Drill.

This section describes how to create a basic group suitable for testing. See later sections for the type.

For a basic group, you need only specify the group type and the number of Drillbits to launch:  

              cluster: [
                  {
                    name: "mypool"
                    type: "basic"
                    count: 1
                 }
              ] 


The above syntax says that pools is a list that contains a series of pool objects contained in
braces. In this release, however, Drill supports just one pool.  

###ZooKeeper Configuration
Drill uses ZooKeeper to coordinate between Drillbits. When run under YARN, the Drill
Application Master uses ZooKeeper to monitor Drillbit health. Drill-on-YARN reads your
$DRILL_SITE/drilloverride.conf file for ZooKeeper settings.  


###Configure the Drill Distribution Archive
Next configure the name of the Drill distribution archive that you downloaded earlier.  

              drill-install:  {
                     client-path: "archive-path"
                   }

Where archive-path is the location of your archive. In our example, this is $MASTER_DIR/apache-drill.x.y.z.tar.gz. Use the full name of the master directory, not the environment variable. (Substitute your actual version number for x.y.z.)  

###Select the Distributed File System Location
Drill copies your archive onto your distributed file system (such as HDFS) in a location you
provide. Set the DFS options as follows:  

              dfs: {
                     connection: "hdfs://localhost/"
                     dir: "/user/drill"
                 }  

Drill can read the connection information from your Hadoop configuration files ($HADOOP_HOME/etc/hadoop/coresite.xml ). Or, you can specify a connection directly in the Drill cluster configuration file using the connection attribute. Then, choose a DFS file system location. Drill uses “ /user/drill ” by default.  

###Hadoop Location
Apache Drill users must tell Drill-on-YARN the location of your Hadoop install. Set the
HADOOP_HOME environment variable in $DRILL_SITE/drillenv.sh to point to your Hadoop installation:  

              export HADOOP_HOME= /path/to/hadoop-home  

This assumes that Hadoop configuration is in the default location:  

              $HADOOP_HOME/etc/hadoop 

If your configuration is elsewhere, set HADOOP_CONF_DIR instead:  

       export HADOOP_CONF_DIR= /path/to/hadoop-config


