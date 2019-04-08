---
title: "Installing Drill on Windows"
date: 2019-04-08
parent: "Installing Drill in Embedded Mode"
---

Currently, Drill supports 64-bit Windows only.  

##Tools Required  

•	Download and install [JDK](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html). Select the download for Windows x64.  
•	Download and install a utility for unzipping a tar.gz file, such as [7-zip](https://www.7-zip.org/download.html).  

## Setting Up Your Windows Environment  

Before you download and install Drill on your Windows machine, complete the following procedures:  

### Set the JAVA_HOME and PATH environment variables.  
1.	Go to System Properties. 
2.	On the Advanced Tab, click Environment Variables.  
![](https://i.imgur.com/lpytfmu.png)  
3.	Click New, and enter JAVA_HOME as the variable name. For the variable value, enter the path to your JDK installation. Instead of using `Program Files` in the path name, use `progra~1`. This is required because Drill cannot use file paths with spaces.![](https://i.imgur.com/3CUoNNZ.png)  
4.	Click OK to continue. 
5.	In the System Variables section, select Path and then click Edit.![](https://i.imgur.com/nqv68Nu.png)
6.	In the Edit Environment Variable window, click New and enter `%JAVA_HOME%\bin`.![](https://i.imgur.com/2kevwLV.png)
7.	Click OK to continue and exit the System Properties window.    


### Create Drill UDF directories and change the owner.  

You, or the user that will start Drill, must manually create and own UDF directories. The directories must exist before starting Drill for the first time.   

1.	Run the command prompt as administrator, and issue the following commands:  
  
			mkdir "%userprofile%\drill"
			mkdir "%userprofile%\drill\udf"
			mkdir "%userprofile%\drill\udf\registry"
			mkdir "%userprofile%\drill\udf\tmp"
			mkdir "%userprofile%\drill\udf\staging"
			takeown /R /F "%userprofile%\drill"

2.	To verify that you (or the user that will run Drill) owns the directories and files, go to the `"%userprofile%\drill"` directory, right-click on it, and select Properties from the list.  
![](https://i.imgur.com/lLLYOMX.png)
3.	Verify that you are the owner for all the directories within drill, including the /udf, /registry, /tmp, and /staging directories. If you are not the owner, Edit the permissions. 


## Download and Install Drill 

1. Download the latest version of Apache Drill [here](http://www-us.apache.org/dist/drill/drill-1.15.0/apache-drill-1.15.0.tar.gz).
2. Move the downloaded file to the directory where you want to install Drill.
3. Unzip the GZ file using a third-party tool. If the tool you use does not unzip the underlying TAR file as well as the GZ file, perform a second unzip to extract the Drill software. The extraction process creates the installation directory containing the Drill software.  
4. [Start Drill]({{site.baseurl}}/docs/starting-drill-on-windows). 
