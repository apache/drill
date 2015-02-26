---
title: "Developing an Aggregate Function"
parent: "Develop Custom Functions"
---
Create a class within a Java package that implements Drill’s aggregate
interface into the program. Include the required information for the function.
Your function must include data types that Drill supports, such as int or
BigInt. For a list of supported data types, refer to the [SQL Reference](/drill/docs/sql-reference/).

Complete the following steps to create an aggregate function:

  1. Create a Maven project and add the following dependency:
  
		<dependency>
		<groupId>org.apache.drill.exec</groupId>
		<artifactId>drill-java-exec</artifactId>
		<version>1.0.0-m2-incubating-SNAPSHOT</version>
		</dependency>
  2. Create a class that implements the `DrillAggFunc` interface and identify the scope as `FunctionTemplate.FunctionScope.POINT_AGGREGATE`.

	**Example**
	
		@FunctionTemplate(name = "count", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
		public static class BitCount implements DrillAggFunc{
  3. Provide the variables used in the code in the `Param, Workspace, `and `Output` bit holders.

	**Example**
	
		@Param BitHolder in;
		@Workspace BitHolder value;
		@Output BitHolder out;
  4. Include the `setup(), add(), output(),` and `reset()` methods.
	
	**Example**
		public void setup(RecordBatch b) {
		  value = new BitHolder(); 
		    value.value = 0;
		}
		 
		@Override
		public void add() {
		      value.value++;
		}
		@Override
		public void output() {
		  out.value = value.value;
		}
		@Override
		public void reset() {
		 
		    value.value = 0;
  5. Use the maven-source-plugin to compile the sources and classes JAR files. Verify that an empty `drill-module.conf` is included in the resources folder of the JARs.   
Drill searches this module during classpath scanning. If the file is not
included in the resources folder, you can add it to the JAR file or add it to
`etc/drill/conf`.