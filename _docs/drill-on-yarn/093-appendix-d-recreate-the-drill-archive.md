---
title: "Appendix D: Recreate the Drill Archive"
date:  
parent: "Drill-on-YARN"
---   

Previous instructions assume that you make no changes to your Drill installation; that all your site-specific files reside in a separate site directory. Prior Drill versions put all  configurations within the Drill directory. If you chose to continue that pattern, or if you change the Drill installation, you must rebuild the Drill archive and configure Drill-on-YARN
to upload your custom archive in place of the standard archive. The steps below explain the process.  

To change the contents of the Drill archive, you must perform two steps:  

1. Create an archive of the Drill install directory.  
2. Configure Drill-on-YARN to use that archive.   

##Create the Drill Archive
The first step is to create the master archive of your Drill files. Do the following with the master directory as the current directory.  

       cd $MASTER_DIR tar -czf archive-name.tar.gz $DRILL_HOME 

//Replace “archivename” with the name you created.

##Configure Drill-on-YARN
To use the archive,  modify your drill-on-yarn.conf file to identify the archive you must created:  

       drill.yarn.drill-install.client-path: “/path/to/archive-name.tar.gz”  

YARN expects that, when extracting the master directory, that it creates a directory called
archive-name that contains the Drill directories conf, jars, and so on. However, if archive-name is different than the name of the $DRILL_HOME directory, simply configure the correct name of the expanded folder:  

       drill.yarn.drill-install.dir-name: “your-dir-name”  

