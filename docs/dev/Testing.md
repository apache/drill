# Testing

Drill makes extensive use of [JUnit](http://junit.org/junit4/) and other libraries for testing. This page provides pointers to the information you need to work with Drill tests. We don't repeat that information; you will want to follow the links and read the original material to get a complete understanding of the libraries that Drill uses.

# Writing Tests

## General Testing Tools

* [Test Data Sets](TestDataSets.md)
* [Temp Directory Utilities](TempDirectories.md)
* [Testing with JUnit](JUnit.md)
* [Test Logging](TestLogging.md)

## Deprecated Drill Testing Techniques

This is a list of old Drill testing machinery that we have cleaner machinery for now.

* [BaseTestQuery](BaseTestQuery.md): Deprecated, use [ClusterTest](ClusterTest.md) instead.

## Legacy Drill Testing Techniques

This is a list of old Drill testing machinery for which there is no other alternative at the moment.

* [Testing with Physical Plans and Mock Data](LegacyTestingFrameworks.md)

## Latest Drill Testing Techniques

These are all the latest Drill testing Techniques that you should use. If you find one of these approaches insufficient for your use, please enhance them or
file a Jira so that they an be improved.

* [RowSet Framework](RowSetFramework.md)
* [Cluster Fixture Framework](ClusterFixture.md)
* [Operator Fixture Framework](OperatorFixture.md)
* [ClusterTest](ClusterTest.md)
* [Single Operator Unit Test](PhysicalOpUnitTestBase.md)
* [Instantiating Components](InstantiatingComponents.md)
* [Generated Code](GeneratedCode.md)
* [MiniPlanUnitTestBase](../../exec/java-exec/src/test/java/org/apache/drill/exec/physical/unit/MiniPlanUnitTestBase.java)

## Testing Guidelines

When testing an operator or a specific internal mechanism in Drill you should use unit tests and should avoid depending on integration tests. This
approach means that you will have to design your implementation to be decoupled from the rest of Drill. While this seems daunting, in reality it doesn't take
much time and you will spend much less time debugging and stressing about customer bug fixes.

Drill provides several tools to facilitate unit testing. See:

 * [RowSet Framework](RowSetFramework.md)
 * [Operator Fixture Framework](OperatorFixture.md)
 * [Single Operator Unit Test](PhysicalOpUnitTestBase.md)
 * [Instantiating Components](InstantiatingComponents.md)
 * [Generated Code](GeneratedCode.md)
 
The following are some examples of operators which have leveraged these techniques for unit testing:

  * [Lateral Join Operator](../../exec/java-exec/src/test/java/org/apache/drill/exec/physical/impl/join/TestLateralJoinCorrectness.java)
  * [External Store Operator](../../exec/java-exec/src/test/java/org/apache/drill/exec/physical/impl/xsort/managed/TestExternalSortInternals.java)

After the unit tests are implemented and passing, use integration tests to ensure the whole system works. The standard tools for accomplishing integration
testing are:

  * [ClusterFixture](ClusterFixture.md)

The general rule is: test as close to your code as possible, refactoring and breaking dependencies where needed to accomplish this.
On the other hand, if the test is more of a SQL or planner-level concept, then testing at the query level might be fine.

## Categories

Currently Drill uses Travis to run smoke tests for every PR and commit. All of Drill's unit tests cannot be run on Travis because Drill's tests take longer to run than the
maximum allowed container time for the free tier of Travis. In order to decide which tests are run on Travis and which tests are not, tests are categorized using JUnit's
`@Category` annotation. Currently the following categories are excluded from Travis:

  - **SlowTest:** Tests that are slow.
  - **UnlikelyTest:** Tests that cover parts of the code that are rarely or never touched.
  - **SecurityTest:** Corner case tests for security features.
  
To mark a test with a category you can do the following:

```
@Category(SlowTest.class)
public class MyTest {
  // Testing code
}
```

To mark a test with multiple categories you can do the following:

```
@Category({SlowTest.class, SecurityTest.class})
public class MyTest {
  // Testing code
}
```

# Running Tests

Drill tests run in parallel. The model for parallel execution is to divide test classes between multiple
forked test processes. Each test process then runs the test classes assigned to it sequentially.

## Speeding Up Test Runs

There are a couple knobs you can turn to make tests run faster on your machine.

 * **Maven Build Threads**: `-T <num>`
 * **Sure Fire Fork Count**: `-DforkCount=<num>`
 * **Test Categories**
 
### -T

Maven allows you to use multiple threads to compile sub modules. Also when running tests each build
thread forks its own surefire process, so the tests for different submodules are run in parallel. In order
to leverage this use the `-T` flag. By default this option is effectively `1`, so there is only one build thread by default.

Ex. In order to run the build using two maven threads use the following command.

```
mvn -T 2 clean install
```

### -DforkCount

To run tests within a submodule in parallel you can use the `-DforkCount` option. By default this `2`, so two surefire processes are forked for each build thread.

Ex. Run 4 test processes in parallel

```
mvn clean install -DforkCount=4
```

**Note:** The `-DforkCount` option interacts with `-T`. When used together each build thread (`-T`) gets 
`-DforkCount` test processes.

### Running Categories

You can leverage categories to run subsets of the tests. This is useful if you need to test something like a
storage plugin on a remote jenkins server. See the java docs [here](../../common/src/test/java/org/apache/drill/categories/package-info.java) for examples. For a list of all 
the availabe categories go [here](../../common/src/test/java/org/apache/drill/categories).

## Maven

It is often helpful to know the [set of properties](https://cwiki.apache.org/confluence/display/MAVEN/Maven+Properties+Guide) that Maven defines and are available for use in the POM file.

## Surefire Plugin

Drill uses the [Maven Surefire plugin](http://maven.apache.org/components/surefire/maven-surefire-plugin/) to run tests. The root POM contains significant test configuration that you may or may not want to include when running tests in a debugger:
```
          <configuration>
            <argLine>-Xms512m -Xmx3g -Ddrill.exec.http.enabled=false
              -Ddrill.exec.sys.store.provider.local.write=false
              -Dorg.apache.drill.exec.server.Drillbit.system_options=\
               "org.apache.drill.exec.compile.ClassTransformer.scalar_replacement=on"
              -Ddrill.test.query.printing.silent=true
              -Ddrill.catastrophic_to_standard_out=true
              -XX:MaxPermSize=512M -XX:MaxDirectMemorySize=3072M
              -Djava.net.preferIPv4Stack=true
              -Djava.awt.headless=true
              -XX:+CMSClassUnloadingEnabled -ea</argLine>
            <forkCount>${forkCount}</forkCount>
            <reuseForks>true</reuseForks>
            <additionalClasspathElements>
              <additionalClasspathElement>./exec/jdbc/src/test/resources/storage-plugins.json</additionalClasspathElement>
            </additionalClasspathElements>
```

You can [run individual tests](http://maven.apache.org/components/surefire/maven-surefire-plugin/examples/single-test.html) as follows. First, cd into the project that contains the test. Then, run the test. (You'll get an error if you try to run the test from the root Drill directory.)
```
cd /path/to/drill/project
mvn surefire:test -Dtest=TestCircle
```

# Java Assertions

Drill code makes sparse use of Java exceptions, preferring the Guava equivalents. One area that assertions (`-ea` option) does affect is performance: queries run about 10x slower with assertions enabled due to allocator logging. When doing any performance-related work, ensure that assertions are not enabled.

```
class DrillBuf ...
  public boolean release(int decrement) {
    ...
    if (BaseAllocator.DEBUG) {
      historicalLog.recordEvent("release(%d). original value: %d", decrement, refCnt + decrement);
    }
``` 

# IntelliJ

TBA
