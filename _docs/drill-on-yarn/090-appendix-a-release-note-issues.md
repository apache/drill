---
title: "Appendix A: Release Note Issues"
date: 2018-11-02
parent: "Drill-on-YARN"
---  

Drill-on-YARN creates a tighter coupling between Drill and Hadoop than did previous Drill
versions. You should be aware of the following compatibility issues:

##Migrating the $DRILL_HOME/conf/drill-env.sh Script
Prior to Drill 1.8, the drill-env.sh script contained Drill defaults, distribution-specific
settings, and configuration specific to your application (“site”.) In Drill 1.8, the Drill and distribution settings are moved to other locations. The site-specific settings change in format to allow YARN to override them. The following section details the changes you must make if you reuse a drill-env.sh file from a prior release. (If you create a new file, you can skip this section.)  

At the end of this process, your file should contain just two lines for memory settings, plus any additional custom settings you may have added.  

##Memory Settings
Most Drill configuration is done via the Drill configuration file and the configuration registry. However, certain options must be set at the time that Drill starts; such options are configured in the $DRILL_HOME/conf/drill-env.sh file. Under YARN, these settings are set in the
YARN configuration. To ensure that the YARN configuration options are used, you must modify
your existing drill-env.sh file as follows. (If you are installing Drill fresh, and don’t have an existing file, you can skip these steps. The Drill 1.8 and later files already have the correct format.)  

Find the two lines that look like this:  

       DRILL_MAX_DIRECT_MEMORY="8G"
       DRILL_HEAP="4G"  

Replace them with the following two lines:  

       export DRILL_MAX_DIRECT_MEMORY=${DRILL_MAX_DIRECT_MEMORY:" 8G"}
       export DRILL_HEAP=${DRILL_HEAP:" 4G"}  


Copy the actual values from the old lines to the new ones (e.g. the “8G” and “4G” values.)
Those are the values that Drill when use if you launch it outside of YARN. The new lines ensure
that these values are replaced by those set by Drill-on-YARN when running under YARN. 

If you omit this change, then Drill will ignore your memory settings in Drill-on-YARN, resulting in a potential mismatch between the Drill memory settings and the amount of memory requested from YARN.  

##Remove General Drill Settings  

If you are reusing the drill-env.sh from a prior release, find lines similar to the following:  

       export DRILL_JAVA_OPTS="-Xms$DRILL_HEAP -Xmx$DRILL_HEAP
       -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY \
       -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=1G \
       -Ddrill.exec.enable-epoll=true"

Compare these lines to the original drill-env.sh to determine if you modified the lines.
These general Drill defaults now reside in other Drill scripts and can be remove from your
site-specific version of drill-env.sh.

##Remove Distribution-Specific Settings
Some Drill distributions added distribution-specific settings to the drill-env.sh script. Drill 1.8 moves these settings to a new $DRILL_HOME/conf/distrib-env.sh file. Compare drill-env.sh and distrib-env.sh . Lines that occur in both files should be removed from
drill-env.sh. 

If you later find you need to change the settings in distrib-env.sh , copy the line to drill-env.sh and modify the line. Drill reads drill-env.sh after distrib-env.sh, so your site-specific settings will replace the default distribution settings.  

##Hadoop Jar Files
Drill depends on certain Hadoop Java JAR files which the Drill distribution includes in the
$DRILL_HOME/jars/3rdparty directory. Although YARN offers Drill a Java class-path with
the Hadoop jars, Drill uses its own copies instead to ensure Drill runs under the same
configuration with which it was tested. Drill distributions that are part of a complete Hadoop distribution (such as the MapR distribution) have already verified version compatibility for you. If you are assembling your own Hadoop and Drill combination, you should verify that the Hadoop version packaged with Drill is compatible with the version running our YARN cluster.  

##$DRILL_HOME/conf/core-site.xml Issue
Prior versions of Drill included a file in the $DRILL_HOME/conf directory called
core-site.xml. YARN relies on a file with the same name in the Hadoop configuration directory. The Drill copy hides the YARN copy, preventing YARN from operating correctly. For this reason, version 1.8 of Drill renames the example file to core-site-example.xml. When upgrading an existing Drill installation, do not copy the file from your current version of Drill to the new version. If you modified core-site.xml, you should merge your changes with Hadoop’s core-site.xml file.  

##Mac OS setsid Issue
YARN has a bug which prevents Drillbits from properly shutting down when run under YARN on
Mac OS.  



- [YARN-3066](https://issues.apache.org/jira/browse/YARN-3066): Hadoop leaves orphaned tasks running after job is killed.  


You may encounter this problem if you use a Mac to try out the YARN integration for Drill. The
symptom is that you:  
● Start Drill as described below  
● Attempt to stop the Drill cluster as described below  
● Afterwards use jps to list Java processes and find that Drillbit is still running.    

The problem is that the setsid command is not available under MacOS. The workaround is to
use the open source equivalent:  

● Install the [XCode command line tools](https://developer.apple.com/library/content/technotes/tn2339/_index.html).  
● Using git, clone ersatz-ssid from https://github.com/jerrykuch/ersatz-setsid
● Cd into the ersatz-ssid directory and type: `make`  
● Copy the resulting executable into `/usr/bin : sudo cp setsid /usr/bin`  

##Apache YARN Node Labels and Labeled Queues
The Drill-on-YARN feature should work with Apache YARN node labels, but such support is
currently not tested. Early indications are that the Apache YARN label documentation does not
quite match the implementation, and that labels are very tricky. The Drill team is looking forward to community assistance to better support Apache YARN node labels.  

##Apache YARN RM Failure and Recovery
Drill-on-YARN currently does not gracefully handle Apache YARN Resource Manager failure
and recovery. According to the Apache YARN documentation, a failed RM may restart any
in-flight Application Masters, then alert the AM of any in-flight tasks. Drill-on-YARN is not
currently aware of this restart capability. Existing Drillbits will continue to run, at least for a time.They may be reported in the Drill-on-YARN web UI as unmanaged. Presumably,  eventually YARN will kill the old Drillbits at which time Drill-on-YARN should start replacements. This is an area for future improvement based on community experience.  

##Configuring User Authentication
The Drill Documentation describes how to configure user authentication using PAM. Two
revisions are needed for Drill-on-YARN:  
● Configure user authentication for Drill using a site directory  
● Configure Drill-on-YARN authentication  

The existing instructions explain how to configure PAM authentication by changing Drill config
files and adding libraries to the Drill distribution directory. If you use that approach, you must rebuild the Drill software archive as described elsewhere in this document. However, you can simply configure security using the site directory as explained below.  

###Configuring User Authentication for the Drillbit
Existing instructions:  

Untar the file, and copy the libjpam.so file into a directory that does not contain other
Hadoop components.  

Example: /opt/pam/

Revised instructions: You have the option of deploying the library to each node, or allowing YARN to distribute the library. To have YARN do the distribution:  

Create the following directory:  

       $DRILL_SITE/lib 

Untar the file and copy libjpam.so into $DRILL_SITE/lib.  

Existing instructions: Add the following line to <DRILL_HOME>/conf/drill-env.sh, including the
directory where the libjpam.so file is located:  

       export DRILLBIT_JAVA_OPTS="-Djava.library.path=<directory>"
       Example: export DRILLBIT_JAVA_OPTS="-Djava.library.path=/opt/pam/"  

Revised instructions: If you are not using Drill-on-YARN, set a new environment variable in drill-env.sh:  

       export DRILL_JAVA_LIB_PATH=”<directory>”  

If you install the library yourself, either set DRILL_JAVA_LIB_PATH as above, or set the
following in drill-on-yarn.conf:  

       drill.yarn.files: {
              librarypath: "<directory>"
       } 

**Note:** Do not explicitly set DRILLBIT_JAVA_OPTS as you may have done in previous releases; Drill will not know how to add your $DRILL_SITE/lib directory or how to interpret the librarypath item above.  

If you put the library in the $DRILL_SITE/lib directory, Drill-on-YARN automatically
does the necessary configuration; there is nothing more for you to do.  

###Implementing and Configuring a Custom Authenticator
Most of the existing steps are fine, except for step 3. 

Current text: Add the JAR file that you built to the following directory on each Drill node:  

       <DRILLINSTALL_HOME>/jars  

Revised text: Add the JAR file that you built to the following directory on each Drill node:  
 
       $DRILL_SITE/jars  

If running under YARN, you only need to add the jar to the site directory on the node from which you start Drill-on-YARN (which we’ve referred to as $MASTER_DIR.)  

Also, step 5: Restart the Drillbit process on each Drill node.  

       <DRILLINSTALL_HOME>/bin/drillbit.sh restart  

Under YARN, restart the YARN cluster:  

       $DRILL_HOME/bin/drill-on-yarn.sh --site $DRILL_SITE restart  

###Configuring User Authentication for the Application Master
If you configure user authentication for Drill, then user authentication is automatically configured in the Application Master also. Only users with admin privileges can use the AM web UI.  

###Testing User Authentication on the Mac
The [Drill Documentation]({{site.baseurl}}/docs/configuring-user-authentication/) describes how to configure user authentication using PAM, including instructions for downloading a required native library. However, if you are testing security on the Mac, the referenced library does not work on modern Macs. Instead, see the workaround in [DRILL-4756](https://issues.apache.org/jira/browse/DRILL-4756).