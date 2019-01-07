---
title: "DROP FUNCTION USING JAR"
date: 2019-01-07
parent: "SQL Commands"
---

The DROP FUNCTION USING JAR command unregisters a UDF. As of Drill 1.9, you can use this command to unregister a UDF based on the JAR file name. See [Dynamic UDFs]({{site.baseurl}}/docs/dynamic-udfs/) for more information.    

## Syntax

The DROP FUNCTION USING JAR command supports the following syntax:

    DROP FUNCTION USING JAR '<jar_name>.jar';  

## Parameters  

*jar_name*  
The name of the JAR file that contains the UDFs.

## Usage Notes  

- When you issue the DROP FUNCTION USING JAR command, Drill unregisters the UDFs based on the JAR file name and removes the JAR file from the UDF directory. Drill deletes all UDFs associated with the JAR file from the Dynamic UDF registry ([persistent store]({{site.baseurl}}/docs/persistent-configuration-storage/)). Drill returns a message with the list of unregistered UDFs.  
  
- By default, Drill returns a result set when you issue DDL statements, such as DROP FUNCTION USING JAR. If the client tool from which you connect to Drill (via JDBC) does not expect a result set when you issue DDL statements, set the `exec.query.return_result_set_for_ddl` option to false, as shown, to prevent the client from canceling queries:  

		SET `exec.query.return_result_set_for_ddl` = false  
		//This option is available in Drill 1.15 and later.   

	When set to false, Drill returns the affected rows count, and the result set is null. 

## Example

       0: jdbc:drill:zk=local> DROP FUNCTION USING JAR 'simple_functions.jar';  
       +---------------+--------------------------------------------------------------------------------------------------------------+
       | ok   	
       |summary
       +---------------+--------------------------------------------------------------------------------------------------------------+
       | true
       | The following UDFs in jar simple_function.jar have been unregistered: ...                        |
       +---------------+--------------------------------------------------------------------------------------------------------------+