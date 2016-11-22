---
title: "DROP FUNCTION USING JAR"
date: 2016-08-04 00:23:10 UTC
parent: "SQL Commands"
---

The DROP FUNCTION USING JAR command unregisters a UDF. As of Drill 1.9, you can use this command to unregister a UDF based on the JAR file name. See Dynamic UDFs for more information. 

## Syntax

The DROP FUNCTION USING JAR command supports the following syntax:

    DROP FUNCTION USING JAR '<jar_name>.jar';  

## Parameters  

*jar_name*  
The name of the JAR file that contains the UDFs.

## Usage Notes
When you issue the DROP FUNCTION USING JAR command, Drill unregisters the UDFs based on the JAR file name and removes the JAR file from the UDF directory. Drill deletes all UDFs associated with the JAR file from the UDF registry (UDF list stored in ZooKeeper). Drill returns a message with the list of unregistered UDFs. 

## Example

       0: jdbc:drill:zk=local> DROP FUNCTION USING JAR 'simple_functions.jar';  
       +---------------+--------------------------------------------------------------------------------------------------------------+
       | ok   	
       |summary
       +---------------+--------------------------------------------------------------------------------------------------------------+
       | true
       | The following UDFs in jar simple_function.jar have been unregistered: ...                        |
       +---------------+--------------------------------------------------------------------------------------------------------------+