# Testing Generated Code

## Writing Unit Tests For Generated Code

An example of unit testing generated code without running all of Drill is the **priorityQueueOrderingTest()** test in 
[TopNBatchTest](../../exec/java-exec/src/test/java/org/apache/drill/exec/physical/impl/TopN/TopNBatchTest.java). That test tests the 
[PriorityQueueTemplate](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/impl/TopN/PriorityQueueTemplate.java) class separately from the rest of Drill.

The testing of [PriorityQueueTemplate](../../exec/java-exec/src/main/java/org/apache/drill/exec/physical/impl/TopN/PriorityQueueTemplate.java) is mainly accomplished by creating
instances of the following classes:

 * [FunctionLookupContext](../../exec/java-exec/src/main/java/org/apache/drill/exec/expr/fn/FunctionLookupContext.java)
 * [CodeCompiler](../../exec/java-exec/src/main/java/org/apache/drill/exec/compile/CodeCompiler.java)
 
## Creating A [FunctionLookupContext](../../exec/java-exec/src/main/java/org/apache/drill/exec/expr/fn/FunctionLookupContext.java)

```
new FunctionImplementationRegistry(drillConfig)
```

## Creating A [CodeCompiler](../../exec/java-exec/src/main/java/org/apache/drill/exec/compile/CodeCompiler.java)

 1. Create an [OperatorFixture](OperatorFixture.md).
 1. Retrieve the [SystemOptionManager](../../exec/java-exec/src/main/java/org/apache/drill/exec/server/options/SystemOptionManager.java) from the
    [OperatorFixture](OperatorFixture.md).
    ```
    operatorFixture.getOptionManager();
    ```
 1. Create an instance of [CodeCompiler](../../exec/java-exec/src/main/java/org/apache/drill/exec/compile/CodeCompiler.java).
    ```
    new CodeCompiler(drillConfig, optionManager)
    ```

## Debugging Generated Code

It is possible to set break points in generated code.

### Instructions For IntelliJ

 1. File→Project structure…→Modules→distribution→Sources → Add content root 
 1. Chose /tmp/drill/codegen 
 1. Mark it as Sources directory.
 1. Set saveCodeForDebugging(true) for the code generator of interest
 1. Run the unit test of interest
 1. Now some generated classes should appear in Intellij under the distribution module
 1. Set a break point in a generated class and run the unit test in debug mode

### Instructions For Eclipse

 1. To step into the generated code, set a breakpoint just before we call into the setup method.
 1. Step into that method which will step into doSetup. This opens the generated code file.
