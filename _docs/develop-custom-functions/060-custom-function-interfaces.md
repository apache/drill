---
title: "Custom Function Interfaces"
date: 2018-06-27 22:03:09 UTC
parent: "Develop Custom Functions"
---
Implement the Drill interface appropriate for the type of function that you
want to develop. Each interface provides a set of required holders where you
input data types that your function uses and required methods that Drill calls
to perform your function’s operations.

## Simple Function Interface
When you develop a simple function, you implement the `DrillSimpleFunc` interface. The name of the function is determined by the characters that you assign to the name variable. For example, the name for the following simple function is `myaddints`:

    @FunctionTemplate(name = "myaddints", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
    public static class Add1 implements DrillSimpleFunc{

The `nulls = NullHandling.NULL_IF_NULL` variable tells Drill to return NULL values as NULL. For most scenarios, this setting should suffice. If you want to change how Drill handles NULL values, you can change the setting to `nulls = NullHandling.INTERNAL`.

The simple function interface includes the `@Param` and `@Output` holders where you indicate the data types that your function can process.

### @Param Holder

This holder indicates the data type that the function processes as input and determines the number of parameters that your function accepts within the query. For example:

    @Param BigIntHolder input1;
    @Param BigIntHolder input2;

### @Output Holder

This holder indicates the data type that the processing returns. For example:

    @Output BigIntHolder out;

### Setup and Eval Functions

The simple function interface also includes two required methods, setup and eval, that Drill calls when processing a query with the function.

* The setup function performs the initialization and processing that Drill only performs once.
* The eval function contains the code that tells Drill what operations to perform on columns of data. You add your custom code to this method.

## Example
The following example shows the program created for the `myaddints` function:

    package org.apache.drill.udfs;
    import java.util.regex.Matcher;
    import java.util.regex.Pattern;
    import org.apache.drill.exec.expr.DrillAggFunc;
    import org.apache.drill.exec.expr.DrillSimpleFunc;
    import org.apache.drill.exec.expr.annotations.FunctionTemplate;
    import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
    import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
    import org.apache.drill.exec.expr.annotations.Output;
    import org.apache.drill.exec.expr.annotations.Param;
    import org.apache.drill.exec.expr.annotations.Workspace;
    import org.apache.drill.exec.expr.holders.BigIntHolder;
    import org.apache.drill.exec.expr.holders.Float8Holder;
    import org.apache.drill.exec.expr.holders.IntHolder;
    import org.apache.drill.exec.expr.holders.VarCharHolder;
     
    public class MyUdfs {
       
      @FunctionTemplate(name = "myaddints", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
      public static class Add1 implements DrillSimpleFunc{
             
        @Param BigIntHolder input1;
        @Param BigIntHolder input2;
        @Output BigIntHolder out;
        public void setup(){}
             
        public void eval(){
          out.value = input1.value + input2.value;
        }
      }

## Aggregate Function Interface

When you develop an aggregate function, you implement the `DrillAggFunc` interface. The name of the function is determined by the characters that you assign to the name variable. For example, the name for the following aggregate function is `mysecondmin`:

    @FunctionTemplate(name = "mysecondmin", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
    public static class MySecondMin implements DrillAggFunc {

The aggregate function interface includes holders where you indicate the data types that your function can process. This interface includes the @Param and @Output holders previously described and also includes the @Workspace holder. 

### @Workspace holder

This holder indicates the data type used to store intermediate data during processing. For example:

    @Workspace BigIntHolder min;
    @Workspace BigIntHolder secondMin;

The aggregate function interface also includes the following methods that Drill calls when processing a query with the function.

* setup
  Performs the initialization and processing that Drill only performs once.  
* add
  Processes each and every record. It applies the function to each value in a column that Drill processes.
* output
  Returns the final result of the aggregate function; the computed value of the processing applied by the Add method. This is the last method that Drill calls. Drill calls this one time after processing all the records.
* reset
  You provide the code in this method that determines the action Drill takes when data types in a column change from one type to another, for example from int to float. Before processing schema-less data, Drill scans the data and implicitly tries to identify the data type associated with each column. If Drill cannot identify a schema associated with each column, Drill processes a column assuming that the column contains a certain data type. If Drill encounters another data type in the column, Drill calls the reset method to determine how to handle the scenario.

## Example

The following example shows the program created for the `mysecondmin` function. The `mysecondmin` function looks at all the incoming values to determine if there is a new minimum value. When an incoming value is less than the minimum value, the function replaces the second minimum value with the value that was previously the minimum value.

    package org.apache.drill.udfs;
    import java.util.regex.Matcher;
    import java.util.regex.Pattern;
    import org.apache.drill.exec.expr.DrillAggFunc;
    import org.apache.drill.exec.expr.DrillSimpleFunc;
    import org.apache.drill.exec.expr.annotations.FunctionTemplate;
    import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
    import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
    import org.apache.drill.exec.expr.annotations.Output;
    import org.apache.drill.exec.expr.annotations.Param;
    import org.apache.drill.exec.expr.annotations.Workspace;
    import org.apache.drill.exec.expr.holders.BigIntHolder;
    import org.apache.drill.exec.expr.holders.Float8Holder;
    import org.apache.drill.exec.expr.holders.IntHolder;
    import org.apache.drill.exec.expr.holders.VarCharHolder;
     
    public class MyUdfs {
       
      @FunctionTemplate(name = "mysecondmin", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
      public static class MySecondMin implements DrillAggFunc {
        @Param BigIntHolder in;
        @Workspace BigIntHolder min;
        @Workspace BigIntHolder secondMin;
        @Output BigIntHolder out;
        public void setup() {
          min = new BigIntHolder(); 
          secondMin = new BigIntHolder(); 
          min.value = 999999999;
          secondMin.value = 999999999;
        }
         
        @Override
        public void add() {
             
            if (in.value < min.value) {
              secondMin.value = min.value;
              min.value = in.value;
            }
             
        }
        @Override
        public void output() {
          out.value = secondMin.value;
        }
        @Override
        public void reset() {
          min.value = 999999999;
          secondMin.value = 999999999;
        }
        
       }
