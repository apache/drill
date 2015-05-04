---
title: "Installing Drill on Linux and Mac OS X"
parent: "Installing Drill in Embedded Mode"
---
First, check that you [meet the prerequisites]({{site.baseurl}}/docs/embedded-mode-prerequisites), and then install Apache Drill on Linux or Mac OS X:

Complete the following steps to install Drill:  

1. Issue the following command in a terminal to download the latest, stable version of Apache Drill to a directory on your machine, or download Drill from the [Drill web site](http://getdrill.org/drill/download/apache-drill-0.9.0.tar.gz):

        wget http://getdrill.org/drill/download/apache-drill-0.9.0.tar.gz  

2. Copy the downloaded file to the directory where you want to install Drill. 

3. Extract the contents of the Drill tar.gz file. Use sudo if necessary:  

        sudo tar -xvzf apache-drill-0.9.0..tar.gz  

The extraction process creates the installation directory named apache-drill-0.9.0 containing the Drill software.

At this point, you can [start Drill]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x).