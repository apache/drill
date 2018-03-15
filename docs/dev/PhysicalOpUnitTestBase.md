# Single Operator Unit Test

It is possible to run an end to end test of an operator in isolation by extending 
[PhysicalOpUnitTestBase](../../exec/java-exec/src/test/java/org/apache/drill/exec/physical/unit/PhysicalOpUnitTestBase.java).

A simple example of an operator level unit test is the following:

```
public class BasicPhysicalOpUnitTest extends PhysicalOpUnitTestBase {

 @Test
 public void testSimpleProject() {
   Project projectConf = new Project(parseExprs("x+5", "x"), null);
   List<String> jsonBatches = Lists.newArrayList(
       "[{\"x\": 5 },{\"x\": 10 }]",
       "[{\"x\": 20 },{\"x\": 30 },{\"x\": 40 }]");
   opTestBuilder()
       .physicalOperator(projectConf)
       .inputDataStreamJson(jsonBatches)
       .baselineColumns("x")
       .baselineValues(10l)
       .baselineValues(15l)
       .baselineValues(25l)
       .baselineValues(35l)
       .baselineValues(45l)
       .go();
 }
}

```

