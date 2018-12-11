---
title: "CREATE FUNCTION USING JAR"
date: 2018-12-11
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
- Before you issue the CREATE FUNCTION USING JAR command, you must Copy the UDF source and binary JAR files to the DFS staging directory. When you issue this command, Drill uses the JAR file name to register the JAR name in the Dynamic UDF registry ([persistent store]({{site.baseurl}}/docs/persistent-configuration-storage/)) and then copies the source and binary JAR files to the local UDF directory on each drillbit upon request.  
   
- By default, Drill returns a result set when you issue DDL statements, such as CREATE FUNCTION USING JAR. If the client tool from which you connect to Drill (via JDBC) does not expect a result set when you issue DDL statements, set the `exec.return_result_set_for_ddl` option to false, as shown, to prevent the client from canceling queries:  

		SET `exec.return_result_set_for_ddl` = false  
		//This option is available in Drill 1.15 and later.   

	When set to false, Drill returns the affected rows count, and the result set is null.

## Example  
       0: jdbc:drill:zk=local> CREATE FUNCTION USING JAR 'simple_functions.jar';  
       +---------------+--------------------------------------------------------------------------------------------------------------+
       | ok   	
       |summary
       +---------------+--------------------------------------------------------------------------------------------------------------+
       | true
       | The following UDFs in jar simple_function.jar have been registered: ...                        |
       +---------------+--------------------------------------------------------------------------------------------------------------+          