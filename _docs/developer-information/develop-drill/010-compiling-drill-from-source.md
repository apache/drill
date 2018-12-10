---
title: "Compiling Drill from Source"
date: 2018-12-20
parent: "Develop Drill"
---
To develop Drill, you compile Drill from source code and then set up a project
in Eclipse for use as your development environment. To review or contribute to
Drill code, you must complete the steps required to install and use the Drill
patch review tool.

## Prerequisites

  * Apache Maven 3.3.1 or later
  * Oracle or OpenJDK 8 

Run the following commands to verify that you have the correct versions of
Maven and JDK installed:

    java -version
    mvn -version

## 1\. Clone the Repository

    git clone https://gitbox.apache.org/repos/asf/drill.git

## 2\. Compile the Code

    cd drill
    mvn clean install -DskipTests

The tarball appears in distribution/target. Move the tarball to a directory for unpacking, unpack, and then you can connect to Drill and query sample
data or you can connect Drill to your data sources.

  * To connect Drill to your data sources, refer to [Connect to Data Sources]({{ site.baseurl }}/docs/connect-a-data-source-introduction) for instructions.
  * To connect to Drill and query sample data, refer to the following topics:
    * [Starting Drill on Linux and Mac OS X]({{ site.baseurl }}/docs/install-drill)
    * [Query Data ]({{ site.baseurl }}/docs/query-data)

