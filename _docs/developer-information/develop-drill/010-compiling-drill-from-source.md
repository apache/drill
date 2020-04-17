---
title: "Compiling Drill from Source"
date: 2019-01-25
parent: "Develop Drill"
---
To develop Drill, you compile Drill from source code and then set up a project
in Eclipse for use as your development environment. To review or contribute to
Drill code, you must complete the steps required to install and use the Drill
patch review tool.

Starting in Drill 1.15, you can build and deploy Apache Drill on the MapR platform. 

## Prerequisites
Apache Drill requires:  
  
- Apache Maven 3.6.3 or later  
- Oracle or OpenJDK 8   

Apache Drill on the MapR platform also requires:  
  
- MapR 6.x  
- RedHat/CentOS 7.x   

Run the following commands to verify that the correct versions of Maven and JDK are installed:

    java -version
    mvn -version  


## 1\. Clone the Repository

    git clone https://gitbox.apache.org/repos/asf/drill.git

## 2\. Compile the Code  

Change to the drill directory:  
  
    cd drill  

Build Apache Drill:  
  
    mvn clean install -DskipTests  

If you want to deploy Apache Drill on the MapR platform, include the `-Pmapr` option to build Drill under the MapR profile:  
 
    mvn clean install -DskipTests -Pmapr  

A tarball is built and appears in the distribution/target directory. Move the tarball to another directory for unpacking. Unpack the tarball and then connect to Drill and query sample data, or connect Drill to your data sources.

  * To connect Drill to your data sources, refer to [Connect to Data Sources]({{ site.baseurl }}/docs/connect-a-data-source-introduction) for instructions.
  * To connect to Drill and query sample data, refer to the following topics:
    * [Starting Drill on Linux and Mac OS X]({{ site.baseurl }}/docs/install-drill)
    * [Query Data ]({{ site.baseurl }}/docs/query-data)

