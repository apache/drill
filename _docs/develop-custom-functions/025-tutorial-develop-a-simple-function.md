---
title: "Tutorial: Develop a Simple Function"
date: 2016-11-18 21:59:16 UTC
parent: "Develop Custom Functions"
---

You can create complex functions having advanced features, but for example purposes, this tutorial covers how to create a simple MASK function. You extend the Drill query engine using the [Drill Simple Function interface](https://github.com/apache/drill/blob/master/exec/java-exec/src/main/java/org/apache/drill/exec/expr/DrillSimpleFunc.java):

```java
package org.apache.drill.exec.expr;

import org.apache.drill.exec.record.RecordBatch;

public interface DrillSimpleFunc extends DrillFunc{
  public void setup();
  public void eval();
}
```

The MASK function transforms the value of a column on each row. The function masks characters in a string, as shown by the following pseudo-code:

`MASK( 'PASSWORD' , '#' , 4 ) => ####WORD`

The MASK function replaces a given number of characters from the beginning of a string with another character. In this example, MASK replaces four characters with the # character. 

You can get the complete project for creating and building this function from the [drill-simple-mask-function Github repository](https://github.com/tgrall/drill-simple-mask-function). 

## Prerequisites

* [Oracle Java SE Development (JDK) Kit 7](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html) or later  
* [Apache Drill 1.1](http://getdrill.org/drill/download/apache-drill-1.1.0.tar.gz) or later  
* [Maven 3.0](https://maven.apache.org/download.cgi) or later

----------

## Step 1: Add dependencies

First, add the following Drill dependency to your maven project:

```xml
 <dependency>
     <groupId>org.apache.drill.exec</groupId>
     <artifactId>drill-java-exec</artifactId>
     <version>1.1.0</version>
 </dependency>
```
----------

## Step 2: Add annotations to the function template

To start implementing the DrillSimpleFunc interface, add the following annotations to the @FunctionTemplate declaration:

* Name of the custom function 
  `name="mask"`
* Scope of the custom function, in this case, Simple 
  `scope= FunctionTemplate.FunctionScope.SIMPLE`
* What to do when the value is NULL, in this case Reverse will just returns NULL
  `nulls = FunctionTemplate.NullHandling.NULL_IF_NULL`

```java
. . .
package org.apache.drill.contrib.function;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;

@FunctionTemplate(
        name="mask",
        scope= FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class SimpleMaskFunc implements DrillSimpleFunc{

    public void setup() {

    }

    public void eval() {

    }
}
```

----------

## Step 3: Declare input parameters

The function will be generated dynamically, as you can see in the [DrillSimpleFuncHolder](https://github.com/apache/drill/blob/master/exec/java-exec/src/main/java/org/apache/drill/exec/expr/fn/DrillSimpleFuncHolder.java/#L42), and the input parameters and output holders are defined using holders by annotations. Define the parameters using the @Param annotation. 

* A nullable string  
* The mask char or string  
* The number of characters to replace starting from the first  

Use a holder classes to provide a buffer to manage larger objects in an efficient way: VarCharHolder or NullableVarCharHolder. 

```java
. . .
public class SimpleMaskFunc implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder input;

    @Param(constant = true)
    VarCharHolder mask;

    @Param(constant = true)
    IntHolder toReplace;
. . .
}
``` 

{% include startnote.html %}Drill doesn’t actually use the Java heap for data being processed in a query but instead keeps this data off the heap and manages the life-cycle for us without using the Java garbage collector.{% include endnote.html %}


----------

## Step 4: Declare the return value type

Also, using the @Output annotation, define the returned value as VarCharHolder type. Because you are manipulating a VarChar, you also have to inject a buffer that Drill uses for the output. 

```java

public class SimpleMaskFunc implements DrillSimpleFunc {
. . .
    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;
. . .
}
```

----------

## Step 5: Implement the eval() method

The MASK function does not require any setup, so you do not need to define the setup() method. Define only the eval() method. 

```java
public void eval() {

    // get the value and replace with
    String maskValue = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(mask);
    String stringValue = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);

    int numberOfCharToReplace = Math.min(toReplace.value, stringValue.length());

    // build the mask substring
    String maskSubString = com.google.common.base.Strings.repeat(maskValue, numberOfCharToReplace);
    String outputValue = (new StringBuilder(maskSubString)).append(stringValue.substring(numberOfCharToReplace)).toString();

    // put the output value in the out buffer
    out.buffer = buffer;
    out.start = 0;
    out.end = outputValue.getBytes().length;
    buffer.setBytes(0, outputValue.getBytes());
}
```

The eval() method performs the following tasks:

* Gets the mask
* Gets the value
* Gets the number of character to replace
* Generates a new string with masked values
* Creates and populates the output buffer

Even to a seasoned Java developer, the eval() method might look a bit strange because Drill generates the final code on the fly to fulfill a query request. This technique leverages Java’s just-in-time (JIT) compiler for maximum speed.

## Basic Coding Rules
To leverage Java’s just-in-time (JIT) compiler for maximum speed, you need to adhere to some basic rules.

* Do not use imports. Instead, use the fully qualified class name as required by the Google Guava API packaged in Apache Drill and as shown in ["Step 3: Declare input parameters"]({{site.baseurl}}/docs/tutorial-develop-a-simple-function/#step-3:-declare-input-parameters).  
* Manipulate the ValueHolders classes, for example VarCharHolder and IntHolder, as structs by calling helper methods, such as getStringFromVarCharHolder and toStringFromUTF8 as shown in ["Step 5: Implement the eval() function"]({{site.baseurl}}/docs/tutorial-develop-a-simple-function/#step-5:-implement-the-eval()-method).  
* Do not call methods such as toString because this causes serious problems.


## Complete Code Listing

The Github drill-simple-mask-function project includes the [complete listing](https://github.com/tgrall/drill-simple-mask-function/blob/master/src/main/java/org/apache/drill/contrib/function/SimpleMaskFunc.java) of the code for the MASK function. 

## Prepare the Package

Because Drill generates the source, you must prepare your package such that classes and sources of the function are present in the classpath. The classes and sources are required for the necessary code generation. Drill uses the compiled code to access the annotations and uses the source code to do code generation. This packaging differs from the way Java code is typically packaged.

For simplicity, use maven to build your project in a pom.xml file as follows:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-source-plugin</artifactId>
    <version>2.4</version>
    <executions>
        <execution>
            <id>attach-sources</id>
            <phase>package</phase>
            <goals>
                <goal>jar-no-fork</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

## Add a drill-module.conf File to Resources
Add a `drill-module.conf` file in the resources folder of your project. The presence of this file tells Drill that your jar contains a custom function. Put the following line in the `drill-module.config`:

`drill.classpath.scanning.packages += "org.apache.drill.contrib.function"`

## Build and Deploy the Function
Build the function using mvn package:

`mvn clean package`

Maven generates two JAR files:

* The default jar with the classes and resources (drill-simple-mask-1.0.jar)  
* A second jar with the sources (drill-simple-mask-1.0-sources.jar)

Add the JAR files to Drill, by copying them to the following location:

`<Drill installation directory>/jars/3rdparty`  

**Note:** This tutorial shows the manual method for adding JAR files to Drill, however as of Drill 1.9, the Dynamic UDF feature provides a new method for users.

## Test the New Function

Restart drill and run the following query on the [`employee.json`]({{site.baseurl}}/docs/querying-json-files/) file installed with Drill:

    SELECT MASK(first_name, '*' , 3) FIRST , MASK(last_name, '#', 7) LAST  FROM cp.`employee.json` LIMIT 5;
    +----------+------------+
    |  FIRST   |    LAST    |
    +----------+------------+
    | ***ri    | ######     |
    | ***rick  | #######    |
    | ***hael  | ######     |
    | ***a     | #######ez  |
    | ***erta  | #######    |
    +----------+------------+
    5 rows selected (2.259 seconds)
