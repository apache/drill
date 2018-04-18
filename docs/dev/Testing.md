# Testing

Drill makes extensive use of [JUnit](http://junit.org/junit4/) and other libraries for testing. This page provides pointers to the information you need to work with Drill tests. We don't repeat that information; you will want to follow the links and read the original material to get a complete understanding of the libraries that Drill uses.

Caveat: information here about Drill is "reverse engineered" from the code; this page has not yet had the benefit of insight from the developers who created Drill's test structure.

# Topics

"Classic" Drill testing techniques

* [Testing with JUnit](JUnit.md)
* [Test Logging](TestLogging.md)
* [Testing with Physical Plans and Mock Data](LegacyTestingFrameworks.md)

"Updated" Drill testing techniques

* [Cluster Fixture Framework](ClusterFixture.md)
* [Operator Fixture Framework](OperatorFixture.md)
* [The Mock Record Reader](MockRecordReader.md)

# Mockito

Drill depends on the [Mockito](http://site.mockito.org) framework. (Need to find & document usages.)

# Maven

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