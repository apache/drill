---
title: "Develop a Simple Function"
parent: "Develop Custom Functions"
---
Create a class within a Java package that implements Drill’s simple interface
into the program, and include the required information for the function type.
Your function must include data types that Drill supports, such as int or
BigInt. For a list of supported data types, refer to the [SQL Reference](/docs/sql-reference).

Complete the following steps to develop a simple function using Drill’s simple
function interface:

  1. Create a Maven project and add the following dependency:
  
		<dependency>
		<groupId>org.apache.drill.exec</groupId>
		<artifactId>drill-java-exec</artifactId>
		<version>1.0.0-m2-incubating-SNAPSHOT</version>
		</dependency>

  2. Create a class that implements the `DrillSimpleFunc` interface and identify the scope as `FunctionScope.SIMPLE`.

	**Example**
	
		@FunctionTemplate(name = "myaddints", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
		  public static class IntIntAdd implements DrillSimpleFunc {

  3. Provide the variables used in the code in the `Param` and `Output` bit holders.

	**Example**
	
		@Param IntHolder in1;
		@Param IntHolder in2;
		@Output IntHolder out;

  4. Add the code that performs operations for the function in the `eval()` method.

	**Example**
	
		public void setup(RecordBatch b) {
		}
		public void eval() {
		 out.value = (int) (in1.value + in2.value);
		}

  5. Use the maven-source-plugin to compile the sources and classes JAR files. Verify that an empty `drill-module.conf` is included in the resources folder of the JARs.   
Drill searches this module during classpath scanning. If the file is not
included in the resources folder, you can add it to the JAR file or add it to
`etc/drill/conf`.

