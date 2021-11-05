# Testing with JUnit

Most of us know the basics of JUnit. Drill uses many advanced features that we mention here. Drill uses [JUnit 4](http://junit.org/junit4/), currently version 4.11.

## References

* [Tutorial](https://github.com/junit-team/junit4/wiki/Getting-started) if you are new to JUnit.
* [JUnit Wiki](http://junit.org/junit4/), especially the Usage and Idioms section.
* [Hamcrest Tutorial](http://code.google.com/p/hamcrest/wiki/Tutorial)
* [Hamcrest Java on GitHub](https://github.com/hamcrest/JavaHamcrest)
* [Understanding JUnit method order execution](https://garygregory.wordpress.com/2011/09/25/understaning-junit-method-order-execution/). Good overview of how the before/after annotations work.
* See also the [update](https://garygregory.wordpress.com/2013/01/23/understanding-junit-method-order-execution-updated-for-version-4-11/) to the above.
* [Using JUnit with Maven](https://github.com/junit-team/junit4/wiki/Use-with-Maven)

## JUnit/Hamcrest Idioms

Drill tests use the JUnit 4 series that uses annotations to identify tests. Drill makes use of the "Hamcrest" additions (which seem to have come from a separate project, later merged into JUnit, hence the strange naming.) Basic rules:

* All tests are packaged into classes, all classes start or end with the word "Test". In Drill, most tests use the prefix format: "TestMumble".
* Test methods are indicted with `@Test`.
* Disabled tests are indicated with [`@Ignore("reason for ignoring")`](https://github.com/junit-team/junit4/wiki/Ignoring-tests)
* Tests use "classic" [JUnit assertions](https://github.com/junit-team/junit4/wiki/Assertions) such as `assertEquals(expected,actual,opt_msg)`.
* Tests also use the newer ["Hamcrest" `assertThat`](https://github.com/junit-team/junit4/wiki/Matchers-and-assertthat) formulation. The Hamcrest project provided a system based on assertions and matchers that are quite handy for cases that are cumbersome with the JUnit-Style assertions.
* Many tests make use of the [test fixture](https://github.com/junit-team/junit4/wiki/Test-fixtures) annotations. These include methods marked to run before or after all tests in a class (`@BeforeClass` and `@AfterClass`) and those that run before or after each test (`@Before` and `@After`).
* The base `DrillTest` class uses the [`ExceptionRule`](https://github.com/junit-team/junit4/wiki/Rules#expectedexception-rules) to declare that no test should throw an exception.
* Some Drill tests verify exceptions directly using the `expected` parameter of `@Test`:
```
  @Test(expected = ExpressionParsingException.class)
  public void testSomething( ) {
```
* Other code uses the [try/catch idiom](https://github.com/junit-team/junit4/wiki/Exception-testing#deeper-testing-of-the-exception).
* Drill tests have the potential to run for a long time, or hang, if thing go wrong. To prevent this, Drill tests use a [timeout](https://github.com/junit-team/junit4/wiki/Timeout-for-tests). The main Drill test base class, `DrillTest` uses a [timeout rule](https://github.com/junit-team/junit4/wiki/Rules#timeout-rule) to set a default timeout of 50 seconds:
```
@Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(50000);
```
* Individual tests (override?) this rule with the timeout parameter to the Test annotation `@Test(timeout=1000)`. This form an only decrease (but not increase) the timeout set by the timeout rule.
* Tests that need a temporary file system folder use the [`@TemporaryFolder` rule](https://github.com/junit-team/junit4/wiki/Rules#temporaryfolder-rule).
* The base `DrillTest` class uses the [`TestName` rule](https://github.com/junit-team/junit4/wiki/Rules#testname-rule) to make the current test name available to code: `System.out.println( TEST_NAME );`.

## Additional Resources

Some other resources that may be of interest moving forward:

* [JUnitParams](https://github.com/Pragmatists/JUnitParams) - a cleaner way to parameterize tests.
* [Assumptions](https://github.com/junit-team/junit4/wiki/Assumptions-with-assume) for declaring dependencies and environment setup that a test assumes.
* [JUnit Rules](https://github.com/junit-team/junit4/wiki/Rules) may occasionally be helpful for specialized tests.
* [Categories](https://github.com/junit-team/junit4/wiki/Categories) to, perhaps, identify those "smoke" tests that should be run frequently, and a larger, more costly set of "full" tests to be run before commits, etc.
* [System Rules][http://stefanbirkner.github.io/system-rules/] - 
A collection of JUnit rules for testing code that uses `java.lang.System` such as printing to `System.out`, environment variables, etc.
* The [`Stopwatch` rule](https://github.com/junit-team/junit4/blob/master/doc/ReleaseNotes4.12.md#pull-request-552-pull-request-937-stopwatch-rule) added in JUnit 4.12 to measure the time a test takes.
* the [`DisableonDebug` rule](https://github.com/junit-team/junit4/blob/master/doc/ReleaseNotes4.12.md#pull-request-956-disableondebug-rule) added in JUnit 4.12 which can turn off other rules when needed in a debug session (to prevent, say, timeouts, etc.)

## JUnit with Maven

The root Drill `pom.xml` declares a test-time dependency on [JUnit 4.11](https://github.com/junit-team/junit4/wiki/Use-with-Maven):

```
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.11</version>
      <scope>test</scope>
    </dependency>
```

Since this dependency is in the root POM, there is no need to add it to the POM files of each Drill module.

## JUnit with Eclipse

Using JUnit with Eclipse is trivial:

* To run all tests in a class, select the class name (or ensure no text is selected) and use the context menu option Debug As... --> JUnit.
* To run a single test, select the name of the test method, and invoke the same menu command.

It is necessary to have Eclipse run on the same version of Java as Drill.

To use Java 8:
```
-vm
/Library/Java/JavaVirtualMachines/jdk1.8.0_102.jdk/Contents/Home/bin
```