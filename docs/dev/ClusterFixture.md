# ClusterFixture

Drill provides two ways to test. The original tests are based on the `BaseTestQuery` are are (or will be) described elsewhere. Limitations of this class prompted creation of a new framework, which this page describes.

* A single base class `BaseTestQuery` holds a large amount of functionality, making it hard to create specialized test classes. One either starts with `BaseTestQuery`, or must replicate that functionality. Since one often has to create specialized setups, this was a bit of a limitation.
* `BaseTestQuery` is very handy in that it starts an embedded Drillbit. But, it does so using a fixed set of boot options. To change the boot options as needed for some tests, one has to allow the initial Drillbit to start, then shut it down, create the new config, and restart. This is tedious when using tests for debugging.
* The set of tools provided by `BaseTestQuery` left some holes: data verification must be in the form of a query, it was hard to run a query and print the results for debugging, and so on.

The "cluster" fixture framework solves these issues by taking a different approach:

* Test functionality is not part of the test class hierarchy. Instead, it is something that tests can use as needed, allowing tests to use whatever test class hierarchy is needed for the job at hand.
* Allows very simply means to set up the config, session and system options needed for a tests.
* Allows starting and stoping the Drillbit as needed: one Drillbit for the entire test, or one per test case. Even allows multiple Drillbits for concurrency tests.
* Provides a wide range of tools to execute queries and inspect results. Includes not just the `TestBuilder` functionality from `BaseTestQuery`, but also a new `QueryBuilder` that provides additional options.

That is the background. Let's see how to use this in practice. The best place to start is with the `ExampleTest` class.

# Simplest Case: Run Query and Print

The simplest test case is one that runs a query and prints the results to CSV. While not a true test case, this is often a handy way to get started on a project or bug fix. It also illustrates the basic needed for advanced cases.

```
public class ExampleTest {
  @Test
  public void firstTest() throws Exception {
    try (ClusterFixture cluster = ClusterFixture.standardCluster();
         ClientFixture client = cluster.clientFixture()) {
      client.queryBuilder().sql("SELECT * FROM `cp`.`employee.json` LIMIT 10").printCsv();
    }
  }
}
```

Let's look at each piece. Every test needs two critical components:

```
ClusterFixture cluster = ...
ClientFixture client = ...
```

* The cluster fixture which represents your embedded Drill cluster. Most often the "cluster" is a single Drillbit, as is the case here. But, the cluster can include multiple Drillbits (coordinated either via Zookeeper or an embededded cluster coordinator.) For now, let's use a single Drillbit.
* The client fixture which represents your Drill client application. The client fixture provides a wealth of functionality that a client may need. Here we only use the ability to run a query.

As your tests grow, you will find the need to set options: often on the server, but sometimes on the client. The two fixtures provide "builder" that help you set up both the client and server the way you want. Here, we use default builders that use the reasonable set of default options.

```
ClusterFixture cluster = ClusterFixture.standardCluster();
ClientFixture client = cluster.clientFixture();
```

We want tests to succeed, but sometimes they fail. In fact, some tests even want to test failures (that, say, the server catches configuration mistakes.) To ensure cleanup, we use the try-with-resources idiom to clean up if anything goes wrong.

```
    try (ClusterFixture cluster = ClusterFixture.standardCluster();
         ClientFixture client = cluster.clientFixture()) {
      // Your test here
    }

```

Next, we want to run a query. Drill has many ways to run a query, and many ways to process the query results. Rather than provide zillions of functions, the client fixture provides a "query builder" that lets you walk through the steps to build and run the query. In the example above, we build the query from a SQL statement, then run it synchronously and print the results to CSV.

```
 // Create a query builder for our Drill client
client.queryBuilder()
   // Run a SQL statement
  .sql("SELECT * FROM `cp`.`employee.json` LIMIT 10")
  // Print the results as CSV
  .printCsv();
```

The best thing at this point is to try the above test case. Create a new JUnit test case, copy the test case from `ExampleTest` (including imports) and run it as a JUnit test case. If there are any glitches, now is the time to catch them.

# Setting Boot Options

By default, the cluster fixture builder sets a standard set of boot options. These include:

* The same options set on the command line in the SureFire setup in the root `pom.xml` file.
* Adjusts some thread counts to a smaller size to allow faster Drillbit start in tests.
* Adjusts some local directory paths.

You can see the full set of options in `ClusterFixture.TEST_CONFIGURATIONS`.

But, often you want to set up boot options in some special way. To do that, just use the cluster fixture builder. Suppose we want to set the slice target to 10:

```
  @Test
  public void secondTest() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.SLICE_TARGET, 10)
        ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
```

The above uses the `configProperty()` method to set the config property as a name/value pair. The name can be a string. But, often it is a bit more maintainable to use the constant declaration for the property, here we use one defined in `ExecConstants`.

The `configProperty()` method has another convenience: you can pass the value as a Java value, not just as a string. For example, above we passed the value as an integer. You can also use strings, doubles, Booleans and other types.

# Setting System Options

Drill provides both boot and system options. System options can be set at the system or session level by running SQL statements. But, it is often cleaner to simply declare the desired session options as part of the test setup, using the same cluster fixture above:

```
  @Test
  public void fourthTest() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        // Easy way to run single threaded for easy debugging
        .maxParallelization(1)
        // Set some session options
        .systemOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY, 2L * 1024 * 1024 * 1024)
        .systemOption(PlannerSettings.EXCHANGE.getOptionName(), true)
        .systemOption(PlannerSettings.HASHAGG.getOptionName(), false)
        ;
```

The above uses some session options defined in `PlannerSettings` as `OptionValidator`s. We need the name which we get using `getOptionName()`.

In some cases, you may want to change an option in a test. Rather than writing out the `ALTER SESSION` statement, you can use a shortcut:

```
    client.alterSession(PlannerSettings.EXCHANGE.getOptionName(), false);
```

Again, you can pass a Java value which the test code will convert to a string, then will build the `ALTER SESSION` command.

# The Mock Data Source

The test framework provides a [mock data source](The Mock Record Reader) that is sometimes handy, especially when you need to generate a large amount of data for, say, testing a sort or aggregation. The test framework automatically defines the required storage plugin:

```
  public void thirdTest() throws Exception {
    ...
      String sql = "SELECT id_i, name_s10 FROM `mock`.`employees_5`";
      client.queryBuilder().sql(sql).printCsv();
```

# Defining A Storage Plugin Configuration

It is often very handy, during development, to accumulate a collection of test files in a directory somewhere. To use them, you can define an ad-hoc storage plugin configuration:

```
  @Test
  public void fourthTest() throws Exception {
    ...
      cluster.defineWorkspace("dfs", "data", "/tmp/drill-test", "psv");
      String sql = "select * from `dfs.data`.`example.tbl` order by columns[0]";
      QuerySummary results = client.queryBuilder().sql(sql).run();
```

`defineWorkspace()` arguments are:

* The (existing) storage plugin
* The workspace name you want to use
* The (local) file system location
* The default format

# Additional Query Tools

As shown above, the query build provides a number of tools.

* Run the query and produce a results summary (with row count, batch count and run time): `run()`
* Run the query and produce a single integer (from the first column of the first row): `singletonInt()`. (Also avalable for longs and strings.)
* Run a query from a logical or physical plan.
* Run a query with an asynchronous listener (rather than waiting for completion as shown thus far.)
* Explain the query (instead of running) with results in either JSON or text.

The following is an example of getting a plan explanation:

```
      System.out.println(client.queryBuilder().sql(sql).explainJson());
```

See the `QueryBuilder` class for these options and more. This class has ample Javadoc to help. (But, let us know if anything is missing.)

# Controlling Logging

Often, during debugging, you want to view the log messages, at debug or trace level, but only for one class or module. You could edit your `logback.xml` file, run the query, and view the result log file. Or, you can just log the desired messages directly to your console.

```
  @Test
  public void fourthTest() throws Exception {
    LogFixtureBuilder logBuilder = LogFixture.builder()
        // Log to the console for debugging convenience
        .toConsole()
        // All debug messages in the xsort package
        .logger("org.apache.drill.exec.physical.impl.xsort", Level.DEBUG)
        // And trace messages for one class.
        .logger(ExternalSortBatch.class, Level.TRACE)
        ;
    ...
    try (LogFixture logs = logBuilder.build();
         ...

```

Here, you use the `LogFixtureBuilder` to set up ad-hoc logging options. The above logs to the console, but only for the `org.apache.drill.exec.physical.impl.xsort` package and `ExternalSortBatch` class. The try-with-resources block sets up logging for the duration of the test, then puts the settings back to the original state at completion.

Note that this fixture only works if you create a `drill-java-exec/src/main/resources/logback-test.xml` file with the following contents:

```
<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n
      </pattern>
    </encoder>
  </appender>
  <logger name="org.apache.drill" additivity="false">
    <level value="error" />
    <appender-ref ref="STDOUT" />
  </logger>
  <root>
    <level value="error" />
    <appender-ref ref="STDOUT" />
  </root>
</configuration>
```

This file turns off all but error logging by default. It does not include the `SOCKET` setup used by folks who like the "Lilith" system.

The reason you must create this file is that Logback makes it very difficult to set one log level for the `SOCKET` appender, another for `STDOUT`. And, for the `LogFixture` to work, logging must start with minimum logging so that the fixture adds more detailed configuration; the fixture can't take away existing configuration. (This is something we hope to improve when time allows.)

In practice, it just means you get the `logback-test.xml` file to work, and copy it into each new branch, but don't include it in your pull requests.

# Using the Test Builder

Most Drill tests use a class called `TestBuilder`. Previously, only tests derived from `BaseTestQuery` could use the test builder. You can also use the test builder with the cluster fixture:

```
    client.testBuilder(). ...
```

This particular feature makes it easy to convert existing `BaseTestQuery` cases to use the new framework.

# Going Further

The above will solve 80% of your needs to run a query to exercise some particular bit of code. The framework can be used for ad-hoc tests used in development, or for permanent unit tests. In particular, this author constantly uses ad-hoc tests with Eclipse to allow very fast edit/compile/debug cycles.

But, there are times when you may have specialized needs. The test framework has many other features useful for more advanced cases:

* Setting up a "mini cluster" with two or more Drillbits which can be coordinated using an in-process or external Zookeeper.
* Gather the query profile, parse it, and print a summary. (Very useful when optimizing code.)
* Return results as a `RowSet` that allows programmatic inspection of values, in-process validation of results, and so on.

If you find you have a special need, poke around the test framework code to see if the feature is already available. If not, feel free to add the feature and post a pull request.