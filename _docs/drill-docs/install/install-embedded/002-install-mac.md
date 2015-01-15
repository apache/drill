---
title: "Installing Drill on Mac OS X"
parent: "Installing Drill in Embedded Mode"
---
Complete the following steps to install Apache Drill on a machine running Mac
OS X:

  1. Open a Terminal window, and create a `drill` directory inside your home directory (or in some other location if you prefer).

     **Example**

        $ pwd
        /Users/max
        $ mkdir drill
        $ cd drill
        $ pwd
        /Users/max/drill

  2. Click the following link to download the latest, stable version of Apache Drill:
  
     [http://www.apache.org/dyn/closer.cgi/drill/drill-0.7.0/apache-drill-0.7.0.tar.gz](http://www.apache.org/dyn/closer.cgi/drill/drill-0.7.0/apache-drill-0.7.0.tar.gz)

  3. Open the downloaded `TAR` file with the Mac Archive utility or a similar tool for unzipping files.

  4. Move the resulting `apache-drill-<version>` folder into the `drill` directory that you created.

  5. Issue the following command to navigate to the `apache-drill-<version>` directory:
  
        cd /Users/max/drill/apache-drill-<version>

At this point, you can [invoke SQLLine](https://cwiki.apache.org/confluence/pa
ges/viewpage.action?pageId=44994063#Starting/StoppingDrill-invokeSQLLine) to
run Drill.