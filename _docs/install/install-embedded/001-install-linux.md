---
title: "Installing Drill on Linux"
parent: "Installing Drill in Embedded Mode"
---
Complete the following steps to install Apache Drill on a machine running
Linux:

  1. Issue the following command to download the latest, stable version of Apache Drill to a directory on your machine:
    
        wget http://www.apache.org/dyn/closer.cgi/drill/drill-0.7.0/apache-drill-0.7.0.tar.gz
  2. Issue the following command to create a new directory to which you can extract the contents of the Drill `tar.gz` file:
  
        sudo mkdir -p /opt/drill
  3. Navigate to the directory where you downloaded the Drill `tar.gz` file.
  4. Issue the following command to extract the contents of the Drill `tar.gz` file to the directory you created:
  
        sudo tar -xvzf apache-drill-<version>.tar.gz -C /opt/drill
  5. Issue the following command to navigate to the Drill installation directory:

        cd /opt/drill/apache-drill-<version>
At this point, you can [invoke
SQLLine](/docs/starting-stopping-drill) to run Drill.