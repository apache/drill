---
title: "Manually Adding Custom Functions to Drill"
date: 2016-10-04 23:35:34 UTC
parent: "Adding Custom Functions to Drill"
---

Administrators can manually add custom functions to Drill. After the custom function is developed, generate the sources and classes JAR files. Add both JAR files to the Drill classpath on each node, and include the name of the package that contains the classes to the main Drill configuration file. Restart the drillbit on each node to refresh the configuration.

To add a custom function to Drill, complete the following steps:

1.	Add the sources and classes JAR file for the custom function to the Drill classpath on all drillbits by copying the files to `<drill installation directory>/jars/3rdparty`.
2.	Include a `drill-module.conf` file in the class JAR file, at its root. 
3.	Add the following code to `drill-module.conf` (src/main/resources/drill-module.conf), and replace `com.yourgroupidentifier.udf` with the package name(s) of your UDF(s), as shown below:

             drill.classpath.scanning.packages += "com.yourgroupidentifier.udf"
**Note:** Separate package names with a comma.
4.	Verify that `DRILL_HOME/conf/drill-override.conf` does not contain any information regarding UDF packages. 
5.	Issue the following command to restart Drill:  

              <drill_installation_directory>/bin/drillbit.sh restart

     Now, you can use the custom function(s) in Drill queries.

