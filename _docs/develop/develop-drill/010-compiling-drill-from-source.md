---
title: "Compiling Drill from Source"
parent: "Develop Drill"
---
To develop Drill, you compile Drill from source code and then set up a project
in Eclipse for use as your development environment. To review or contribute to
Drill code, you must complete the steps required to install and use the Drill
patch review tool.

## Prerequisites

  * Maven 3.0.4 or later
  * Oracle JDK 7 or later

Run the following commands to verify that you have the correct versions of
Maven and JDK installed:

    java -version
    mvn -version

## 1\. Clone the Repository

    git clone https://git-wip-us.apache.org/repos/asf/drill.git

## 2\. Compile the Code

    cd incubator-drill
    mvn clean install -DskipTests

## 3\. Explode the Tarball in the Installation Directory

    mkdir ~/compiled-drill
    tar xvzf distribution/target/*.tar.gz --strip=1 -C ~/compiled-drill

Now that you have Drill installed, you can connect to Drill and query sample
data or you can connect Drill to your data sources.

  * To connect Drill to your data sources, refer to [Connect to Data Sources]({{ site.baseurl }}/docs/connect-a-data-source-introduction) for instructions.
  * To connect to Drill and query sample data, refer to the following topics:
    * [Start Drill ]({{ site.baseurl }}/docs/starting-stopping-drill)(For Drill installed in embedded mode)
    * [Query Data ]({{ site.baseurl }}/docs/query-data)

