---
title: "Appendix C: Troubleshooting"
date: 2018-11-02
parent: "Drill-on-YARN"
---  

Drill-on-YARN starts a complex chain of events: the client starts the AM and the AM starts
Drillbits, both using YARN. Many opportunities exist for configuration issues to derail the
process. Below are a number of items to check if things go wrong.  

##Client Start
The Drill-on-YARN client prints all messages to the console. Some common errors are:  

**Missing HADOOP_HOME**    
Drill-on-YARN requires access to your Hadoop configuration as described above. The client will
display an error and fail if it is unable to load the DFS or YARN configuration, typically because
HADOOP_HOME is not set.  

**Missing/wrong Drill Archive**
Drill-on-YARN uploads your Drill archive to DFS. The client will fail if the archive configuration is missing, if the archive does not exist, is not readable, or is not in the correct format. Check that the drill.yarn.drill-install.client-path provides the full path name to the Drill archive.  

**DFS Configuration or Connection Problems**  

The Drill-on-YARN client uploads the Drill archive and your site directory to your DFS. Possible
problems here include:  
● Missing DFS configuration in the Hadoop configuration folder  
● Incorrect DFS configuration (wrong host or port)  
● Missing permissions to write to the folder identified by the drill.yarn.dfs.app-dir configuration property (“/user/drill” by default.)  

**Wrong Version of the Drill Archive**  

Drill-on-YARN uploads a new copy of your site archive each time you start your Drill cluster.
However, the Drill software archive is large, so Drill-on-YARN uploads the archive only when it
changes. Drill detects changes by comparing the size of the DFS copy with your local copy.
Most of the time this works fine. However, if you suspect that Drill has not uploaded the most 
recent copy, you can force the client to perform an upload by either manually deleting the Drill
archive from DFS, or using the f option:  

       $DRILL_HOME/bin/drill-on-yarn.sh --site $DRILL_SITE start -f  

**Site Directory Problems**  

Drill creates a tar archive of your site directory using the following command:  

       tar -C $DRILL_SITE -czf /some/tmp/dir/temp-name.tar.gz  

For some temporary directory selected by Java. This command can fail if your version of tar
does not recognize the above arguments, if the site directory is not readable, or the temporary
file cannot be created.  

**YARN Application Launch Failure**  

YARN may fail to launch the Drill AM for a number of reasons. The user running the
Drill-on-YARN client may not have permission to launch YARN applications. YARN itself may
encounter errors. Check the YARN log files for possible causes.  

**Diagnosing Post-Launch Problems**
If the Drill AM starts, but does not launch Drillbits, your next step is to check the Drill AM web UI using the link provided when the application starts. If the AM exits quickly, then the URL may
not be valid. 

Instead, you can use YARN’s own Resource Manager UI to check the status of the application,
using the Application ID provided when the application starts. Look at the application's log files
for possible problems.  

**Application Master Port Conflict**  

The Drill-on-YARN Application Master provides a web UI on port 8048 (one greater than the Drill
web UI port) by default. However, if another application is already bound to that port, the AM will
fail to launch. Select a different port, as follows:  

       drill.yarn.http.port: 12345

**Multiple AMs**
It is easy to accidentally start multiple AMs for the same Drill cluster. Two lines of defense
protect against this fault:  

The Drill-on-YARN client look for an existing appid file and refuses to start a new AM when the
file is present. (Use the f file if the file is not valid.) The AM registers with ZK and will automatically shut down if another AM is already registered. 


**Drillbit Failure**  
● Finding launch, container, log directories  
● Double drillbit with same ports  
  
Diagnostics  
● Client  
● Client provides AM web UI  
● Finding launch, container, log directories  
● Client status  

Fixing Problems  
● Change config files  
● If Drill archive changes, force new upload  
● Missing app ID file (use a option)  
● Stop existing app (f to force stop)  
● Restart DoY (reuploads config)
