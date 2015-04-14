---
title: "Custom Function Interfaces"
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

The simple function interface includes holders where you indicate the data types that your function can process.

The following table provides a list of the holders and their descriptions, with examples: 

<table><tbody><tr><th ><p><strong>Holder</strong></p></th><th ><p><strong>Description</strong></p></th><th ><p><strong>Example</strong></p></th></tr><tr><td ><pre>@Param</pre></td><td ><p>Indicates the data type that the function processes as input<br /><span>and determines the number of parameters that your function<br />accepts within the query.</span></p></td><td ><pre>@Param BigIntHolder input1;</pre><pre>@Param BigIntHolder input2;</pre></td></tr><tr><td ><pre>@Output</pre></td><td ><p>Indicates the data type that the processing returns.</p></td><td ><pre>@Output BigIntHolder out;</pre><p> </p></td></tr></tbody></table>

The simple function interface also includes two required methods that Drill calls when processing a query with the function.

The following table lists the required methods:

<table><tbody><tr><th><p><strong>Method</strong></p></th><th ><p><strong>Description</strong></p></th></tr><tr><td ><pre>setup()</pre></td><td ><p>Performs the initialization and processing that Drill only performs once.</p></td></tr><tr><td ><pre>eval()</pre></td><td ><p>Contains the code that tells Drill what operations to perform on columns of data. You add your custom code to this method.</p></td></tr></tbody></table>

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
    import org.apache.drill.exec.record.RecordBatch;
     
    public class MyUdfs {
       
      @FunctionTemplate(name = "myaddints", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
      public static class Add1 implements DrillSimpleFunc{
             
        @Param BigIntHolder input1;
        @Param BigIntHolder input2;
        @Output BigIntHolder out;
        public void setup(RecordBatch b){}
             
        public void eval(){
          out.value = input1.value + input2.value;
        }
      }

## Aggregate Function Interface

When you develop an aggregate function, you implement the `DrillAggFunc` interface. The name of the function is determined by the characters that you assign to the name variable. For example, the name for the following aggregate function is `mysecondmin`:

    @FunctionTemplate(name = "mysecondmin", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
    public static class MySecondMin implements DrillAggFunc {

The aggregate function interface includes holders where you indicate the data types that your function can process.

The following table provides a list of the holders and their descriptions, with examples: 

<table ><tbody><tr><th ><p><strong>Holder</strong></p></th><th ><p><strong>Description</strong></p></th><th ><p><strong>Example</strong></p></th></tr><tr><td ><pre>@Param</pre></td><td ><p>Indicates the data type that the function processes as input and determines <br />the number of parameters that your function accepts within the query.<strong> </strong></p></td><td ><pre>@Param BigIntHolder in;</pre></td></tr><tr><td ><pre>@Workspace</pre></td><td ><p>Indicates the data type used to store intermediate data during processing.</p></td><td ><pre>@Workspace BigIntHolder min; @Workspace BigIntHolder secondMin;</pre></td></tr><tr><td ><pre>@Output</pre></td><td ><p>Indicates the data type that the processing returns.</p></td><td ><pre>@Output BigIntHolder out;</pre></td></tr></tbody></table>

The aggregate function interface also includes four required methods that Drill calls when processing a query with the function.

The following table lists the required methods:

<table><tbody><tr><th ><p><strong>Method</strong></p></th><th ><p><strong>Description</strong></p></th></tr><tr><td ><pre>setup()</pre></td><td ><p>Performs the initialization and processing that Drill only performs once.</p></td></tr><tr><td ><pre>add()</pre></td><td ><p>Processes each and every record. It applies the function to each value in a column that Drill processes.</p></td></tr><tr><td ><pre>output()</pre></td><td ><p>Returns the final result of the aggregate function; the computed value of the processing applied by the Add method. <br />This is the last method that Drill calls. Drill calls this one time after processing all the records.</p></td></tr><tr><td ><pre>reset()</pre></td><td ><p>You provide the code in this method that determines the action Drill takes when data types in a column change <br />from one type to another, for example from int to float. <span>Before processing schema-less data, Drill scans the data<br />and implicitly tries to identify the data type associated with </span><span>each column. If Drill cannot identify a schema <br />associated with each column, Drill processes a column assuming that </span><span>the column contains a certain data type. <br />If Drill encounters another data type in the column, Drill calls the reset method </span><span>to determine how to handle the scenario.</span></p></td></tr></tbody></table>

## Example

The following example shows the program created for the `mysecondmin` function:

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
    import org.apache.drill.exec.record.RecordBatch;
     
    public class MyUdfs {
       
      @FunctionTemplate(name = "mysecondmin", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
      public static class MySecondMin implements DrillAggFunc {
        @Param BigIntHolder in;
        @Workspace BigIntHolder min;
        @Workspace BigIntHolder secondMin;
        @Output BigIntHolder out;
        public void setup(RecordBatch b) {
            min = new BigIntHolder(); 
            secondMin = new BigIntHolder(); 
          min.value = 999999999;
          secondMin.value = 999999999;
        }
         
        @Override
        public void add() {
             
            if (in.value < min.value) {
                min.value = in.value;
                secondMin.value = min.value;
            }
             
        }
        @Override
        public void output() {
          out.value = secondMin.value;
        }
        @Override
        public void reset() {
          min.value = 0;
          secondMin.value = 0;
        }
        
       }