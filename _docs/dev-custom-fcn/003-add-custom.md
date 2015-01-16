---
title: "Adding Custom Functions to Drill"
parent: "Develop Custom Functions"
---
After you develop your custom function and generate the sources and classes
JAR files, add both JAR files to the Drill classpath, and include the name of
the package that contains the classes to the main Drill configuration file.
Restart the Drillbit on each node to refresh the configuration.

To add a custom function to Drill, complete the following steps:

  1. Add the sources JAR file and the classes JAR file for the custom function to the Drill classpath on all nodes running a Drillbit. To add the JAR files, copy them to `<drill installation directory>/jars/3rdparty`.
  2. On all nodes running a Drillbit, add the name of the package that contains the classes to the main Drill configuration file in the following location:
  
        <drill installation directory>/conf/drill-override.conf
	To add the package, add the package name to
	`drill.logical.function.package+=`. Separate package names with a comma.
	
    **Example**
		
		drill.logical.function.package+= [“org.apache.drill.exec.expr.fn.impl","org.apache.drill.udfs”]
  3. On each Drill node in the cluster, navigate to the Drill installation directory, and issue the following command to restart the Drillbit:
  
        <drill installation directory>/bin/drillbit.sh restart

     Now you can issue queries with your custom functions to Drill.