---
title: "Dynamic UDFs"
date: 2016-11-21 21:25:57 UTC
parent: "Adding Custom Functions to Drill"
---

Drill 1.9 introduces support for Dynamic UDFs. The Dynamic UDF feature enables users to register and unregister UDFs on their own using the CREATE FUNCTION USING JAR and DROP FUNCTION USING JAR commands.  

The Dynamic UDF feature eliminates the need to restart drillbits, which can disrupt users, when administrators manually load and unload UDFs in a multi-tenant environment. Users can issue the CREATE FUNCTION USING JAR command to register manually loaded (built-in) UDFs. Also, users can migrate registered UDFs to built-in UDFs.  

The Dynamic UDF feature is enabled by default. An administrator can enable or disable the feature using the ALTER SYSTEM SET command with the `exec.udf.enable_dynamic_support option`. When the feature is enabled, users must upload their UDF (source and binary) JAR files to a staging directory in the distributed file system before issuing the CREATE FUNCTION USING JAR command to register a UDF.  

If users do not have write access to the staging directory, the registration attempt fails. When a user issues the CREATE FUNCTION USING JAR command to register a UDF, Drill uses specific directories while validating and registering the UDFs. ZooKeeper stores the list of UDFs and associated JAR files. Drillbits refer to this list when registering and unregistering UDFs.  

##UDF Directories 
 
The directories that Drill uses when registering UDFs are configured in the `drill.exec.udf` stanza of the `drill-override.conf` file. Upon startup, Drill verifies that these directories exist in the file system.  If the directories do not exist, Drill creates them. If Drill is unable to create the directories, the start-up attempt fails. An administrator can modify the directory locations in `drill-override.conf`.

The configuration file contains the following default properties and directories required to use the Dynamic UDF feature:  

       drill.exec.udf: {
                 	retry-attempts: 5,  
       directory: {
          	   	base: ${drill.exec.zk.root}"/udf",
                 	local: ${drill.exec.udf.directory.base}"/local",
                    staging: ${drill.exec.udf.directory.base}"/staging",
                 	registry: ${drill.exec.udf.directory.base}"/registry",
                 	tmp: ${drill.exec.udf.directory.base}"/tmp"
                 	}
       }  

The following table describes the configuration properties and UDF directories, where `drill.exec.udf.directory.base` is the relative directory used to generate all of the UDF directories (local and remote):  

|       Property                                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|---------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| retry-attempts                                          | The   number of times that the UDF registry update can fail before Drill returns an   error. Drill checks the registry version before updating the remote function   registry to avoid overriding changes made by another user. If the registry   version has changed, Drill validates the functions among the updated registry   again. The default is 5.                                                                                                                                                                                                                                              |
| base: ${drill.exec.zk.root}"/udf"                       | The   property used to separate the UDF directories between clusters that use the   same file system.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| local:   ${drill.exec.udf.directory.base}"/local"       | The relative path concatenated   to the Drill temporary directory to indicate the local UDF directory. The   local UDF directory is used as a temporary directory for the Dynamic UDF JAR   files. Drill cleans this directory out upon exiting.                                                                                                                                                                                                                                                                                                                                                        |
| staging:   ${drill.exec.udf.directory.base}"/staging"   | The   location to which users copy their binary and source JAR files. This   directory must be accessible to users in order to register their UDFs. When a   UDF is registered, Drill deletes both of the JAR files (source and binary)   from this directory. If Drill fails to register the UDFs from the JAR files,   the JAR files remain here. You can change the location of this   directory.                                                                                                                                                                                                    |
| registry:   ${drill.exec.udf.directory.base}"/registry" | The   location to which Drill copies the source and binary JAR files after   validating the UDFs. Drill copies the JAR files from the registry directory   to a local UDF directory on each drillbit. When you unregister UDFs, Drill   deletes the appropriate JAR files from the local UDF directory on each   drillbit. DO NOT delete the JAR files from the registry directory. Deleting   JAR files from the registry directory results in inconsistencies in the   Dynamic UDF registry that point to the directory where JAR files are stored.   You can change the location of this directory.  |
| tmp:   ${drill.exec.udf.directory.base}"/tmp"           | The   location to which Drill backs up the binary and source JAR files before   starting the registration process. Drill places each binary and source file   in a unique folder in this directory. At the end of registration, Drill   deletes both JAR files from this directory. You can change the location of   this directory.                                                                                                                                                                                                                                                                    |  

The following table lists optional directories that you can add:  

|       Property                | Description                                                                                                                                                                                                                                                                                                                                 |
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| drill.exec.udf.directory.fs   | Changes the file system from the   default. If there are multiple drillbits in the cluster, and the default file   system is not distributed, you must include this property, and set it to a   distributed file system. For example, file:///, hdfs:///, or maprfs:///, as   shown below:          drill.exec.udf.directory.fs: "hdfs:///" |
| drill.exec.udf.directory.root | Changes   the root directory for remote UDF directories. By default, this property is   set to the home directory of the user that started the drillbit. For example,   on Linux the location is /home/some_user. On DFS, the location is   /user/<user_name>. And, on Windows, the location is   /C:/User/<user_name>.                     |  

##Security and Authentication Impact
Currently, any user can register UDFs if they
have access to the staging directory. Since Drill does not provide full
authorization and authentication support, an administrator may want to disable
the Dynamic UDF feature. See Enabling and Disabling the Dynamic UDF Feature.
 
Drill moves all JAR files from the staging directory to the other UDF directories as the user that started the drillbit, not as the user that submitted the JAR files. Drill behaves this way even if impersonation is enabled.  

##Before Using the Dynamic UDF Feature  
Before users can successfully register and unregister their UDFs using the Dynamic UDF feature, administrators and users must complete certain prerequisites. The prerequisites for each role are listed below.  

###Administrators 
Before users can issue the CREATE FUNCTION USING JAR or DROP FUNCTION USING JAR commands to register or unregister UDFs, verify that the `exec.udf.enable_dynamic_support` option is enabled and that the staging directory is accessible to users. See Enabling and Disabling the Dynamic UDF Feature.  

###Users
Create a UDF using Drill’s [simple]({{site.baseurl}}/docs/developing-a-simple-function/) or [aggregate]({{site.baseurl}}/docs/developing-an-aggregate-function/) function interface. Add a `drill-module.conf` file to the root of the class JAR file. The `drill-module.conf` file should contain the packages to scan for the functions `drill.classpath.scanning.packages+= "com.mydomain.drill.fn"`, as shown in the following example:  

       drill.classpath.scanning.package+= "com.mydomain.drill.fn"  

Replace `com.mydomain.drill.fn` with the package name(s) of your UDF(s). If there are multiple packages, separate package names with a comma.
 
Once the UDF is created, copy the source and binary JAR files to the staging directory. If you do not know the location of the staging directory, contact your administrator. Now, you can register your UDF using the CREATE FUNCTION USING JAR command. See Registering a UDF.  

##Enabling and Disabling the Dynamic UDF Feature
An administrator can enable or disable the Dynamic UDF feature. The feature is enabled by default.  The `exec.udf.enable_dynamic_support` option turns the Dynamic UDF feature on and off. If security is a concern, the administrator can disable the feature to prevent users from registering and unregistering UDFs.


Use the [ALTER SYSTEM SET]({{site.baseurl}}/docs/alter-system/) command with the  `exec.udf.enable_dynamic_support` system option to turn the feature on or off.  

##Registering a UDF
Copy the UDF source and binary JAR files to the DFS staging directory and then issue the CREATE FUNCTION USING JAR command to register the UDF, as follows:   

       CREATE FUNCTION USING JAR ‘<jar_name>.jar’  

If you do not know the location of the staging directory or you need access to the directory, contact your administrator.

When you issue the command, Drill uses the JAR file name to register the JAR name in the Dynamic UDF registry (UDF list stored in ZooKeeper) and then copies the source and binary JAR files to the local UDF directory on each drillbit.  

Upon successful registration, Drill returns a message with a list of registered UDFs:  

       +---------------+-----------------------------------------------------------------------------------------------------------+
       | ok    	      |    summary        	          	                                                                   |
       +---------------+-----------------------------------------------------------------------------------------------------------+
       | true          | The following UDFs in jar %s have been registered: %s                           |
       +---------------+-----------------------------------------------------------------------------------------------------------+  

##Unregistering a UDF
Issue the DROP FUNCTION USING JAR command to unregister a UDF, as follows:  

       DROP FUNCTION USING JAR ‘<jar_name>.jar’  

When you issue the command, Drill unregisters UDFs based on the JAR file name and removes the JAR files from the UDF directory. Drill deletes all UDFs associated with the JAR file from the UDF registry (UDF list stored in ZooKeeper), signaling drillbits to start the local unregistering process.  

Drill returns a message with the list of unregistered UDFs:  

       +---------------+-----------------------------------------------------------------------------------------------------------+
       | ok    	      |    summary        	          	                                                                   |
       +---------------+-----------------------------------------------------------------------------------------------------------+
       | true          | The following UDFs in jar %s have been unregistered: %s                          |
       +---------------+-----------------------------------------------------------------------------------------------------------+  

##Migrating UDFs from Dynamic to Built-In  
 
You can migrate UDFs registered using the Dynamic UDF feature to built-in UDFs to free up space in the UDF directories and the Dynamic UDF registry (UDF list stored in ZooKeeper). You can migrate all of the UDFs or you can migrate a portion of the UDFs. If you migrate all of the UDFs, you cannot issue the DROP FUNCTION USING JAR command to unregister the UDFs that have been migrated from dynamic to built-in.  

###Migrating All Registered UDFs to Built-In UDFs
To migrate all registered UDFs to built-in UDFs, complete the following steps:  

1. Stop all drillbits in the cluster.  
2. Move the UDF source and binary JAR files to the $DRILL_SITE/jars directory on each drillbit. (Must be included in the classpath.)
3. Remove the remote function registry from ZooKeeper.
4. Start all drillbits in the cluster.

###Migrating Some of the Registered UDF JAR Files to Built-In UDFs
To migrate a portion of the UDF JAR files to built-in UDFs, complete the following steps:

1. Copy (not move) the JAR files from the UDF registry directory to the $DRILL_SITE/jars directory on each drillbit. (Must be included in the classpath.)
2. Issue the DROP FUNCTION USING JAR command for each JAR file.
3. Stop all drillbits in the cluster.
4. Start all drillbits in the cluster.  

##Limitations
The Dynamic UDF feature has the following known limitations:  

* If a user drops a UDF while a query that references the UDF is running, the query may fail. Users should verify that no queries reference a UDF prior to issuing the DROP command.
* The DROP command only operates at the JAR level name. A user cannot unregister only one UDF from a JAR where several UDFs are present. To avoid this situation, a user can create one UDF per jar.
* All UDF directories (remote or local) are created upon drillbit startup, even if Dynamic UDF support is disabled. Drillbit startup fails if the user who started the drillbit does not have write access to these directories.






