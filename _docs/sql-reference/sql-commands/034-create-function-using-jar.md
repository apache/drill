---
title: "CREATE FUNCTION USING JAR"
date: 2016-05-05 21:44:33 UTC
parent: "SQL Commands"
---
The CREATE FUNCTION USING JAR command registers UDFs in Drill. See [Dynamic UDFs]({{site.baseurl}}/docs/dynamic-udfs/) for more information.   

## Syntax

The CREATE FUNCTION USING JAR command supports the following syntax:

    CREATE FUNCTION USING JAR '<jar_name>.jar';

## Parameters
*jar_name*  
The name of the JAR file that contains the UDFs.

## Usage Notes  
Before you issue the CREATE FUNCTION USING JAR command, you must Copy the UDF source and binary JAR files to the DFS staging directory. When you issue this command, Drill uses the JAR file name to register the JAR name in the Dynamic UDF registry ([persistent store]({{site.baseurl}}/docs/persistent-configuration-storage/)) and then copies the source and binary JAR files to the local UDF directory on each drillbit upon request. 

## Example  
       0: jdbc:drill:zk=local> CREATE FUNCTION USING JAR 'simple_functions.jar';  
       +---------------+--------------------------------------------------------------------------------------------------------------+
       | ok   	
       |summary
       +---------------+--------------------------------------------------------------------------------------------------------------+
       | true
       | The following UDFs in jar simple_function.jar have been registered: ...                        |
       +---------------+--------------------------------------------------------------------------------------------------------------+          