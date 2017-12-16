---
title: "Installing Drill on Linux and Mac OS X"
date: 2017-12-16 06:24:23 UTC
parent: "Installing Drill in Embedded Mode"
---
First, check that you [meet the prerequisites]({{site.baseurl}}/docs/embedded-mode-prerequisites), and then install Apache Drill on Linux or Mac OS X:

Complete the following steps to install Drill:  

1. In a terminal window, change to the directory where you want to install Drill.  
2. Download the latest version of Apache Drill [here](http://apache.mirrors.hoobly.com/drill/drill-1.12.0/apache-drill-1.12.0.tar.gz) or from the [Apache Drill mirror site](http://www.apache.org/dyn/closer.cgi/drill/drill-1.12.0/apache-drill-1.12.0.tar.gz) with the command appropriate for your system:  
       * `wget http://apache.mirrors.hoobly.com/drill/drill-1.12.0/apache-drill-1.12.0.tar.gz`  
       * `curl -o apache-drill-1.12.0.tar.gz http://www.apache.org/dyn/closer.cgi/drill/drill-1.12.0/apache-drill-1.12.0.tar.gz`  
3. Copy the downloaded file to the directory where you want to install Drill.  
4. Extract the contents of the Drill `.tar.gz` file. Use sudo only if necessary:  
`tar -xvzf <.tar.gz file name>`  

The extraction process creates the installation directory containing the Drill software. You can now [start Drill]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x).
