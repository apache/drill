# Test Logging

Drill code makes extensive use of logging to provide the developer with in-depth information about Drill's internals. The logging is vital when tracking down a problem, but a distraction at other times. To control logging, you must understand the various forms of logging and logging control.

## Logback Classic

Drill uses [Logback Classic](http://logback.qos.ch) as its logger. In production mode, you control logging via the `$DRILL_HOME/conf/logback.xml` (or, alternatively, `$DRILL_SITE/logback.xml`) file. This configuration is _not_ used when testing, however.

When running tests, either in Maven or your IDE, Drill uses a different configuration file: the one in the `src/test/resources` folder for the Maven subproject that holds the test. For example, if you run tests in the `drill-common` project, Logback will find the configuration in `common/src/test/resources/logback.xml`.

Configure logging in this test file. For example, to set the debug log level:
```
  <logger name="org.apache.drill" additivity="false">
    <level value="debug" />
```

Under Maven, each module has visibility to is own test resources, but not the test resources from any other Maven module. To control test logging, therefore, we must have a copy of `logback-test.xml` in the `src/test/resources` folder of each Maven module.

Sometimes it is helpful to verify which configuration file Logback actually uses. As explained in [this Stack Overflow post](http://stackoverflow.com/questions/18922730/how-can-i-determine-what-log-configuration-source-logback-actually-used), you can find this information using a Logback property:
```
mvn surefire:test -Dtest=TestClassPathScanner \
       -Dlogback.statusListenerClass=ch.qos.logback.core.status.OnConsoleStatusListener
```

## Test Logging with Maven

Maven output is controlled by [Maven's logging mechanism](https://maven.apache.org/maven-logging.html) which seems to be [SLF4J Simple](http://www.slf4j.org/apidocs/org/slf4j/impl/SimpleLogger.html).

To "quiet" the amount of Maven output, you must modify the logging configuration file in your Maven installation. (There seems to be no way to modify logging in the POM file itself.)

```
$ cd `dirname \`which mvn\``/../conf/logging
$ pwd # Should print /some/path/apache-maven-3.x.y/conf/logging
$ vi simplelogger.properties
```
The following suppresses all but the most of Maven's "info" logging:
```
#org.slf4j.simpleLogger.defaultLogLevel=info
org.slf4j.simpleLogger.defaultLogLevel=warn
...

# Custom entries
# Display check style issues (shown at info log level)
org.slf4j.simpleLogger.log.org.apache.maven.plugin.checkstyle=info
```

Since the `maven-checkstyle-plugin` displays its errors to the log at the INFO log level, we have to allow it to display info-level messages (the last line in the example.)

Since this is a Maven configuration (not a POM file change), you must make it on each machine on which you run Maven.

As noted above, logging within Drill tests is controlled by Logback, not Maven.

## Logback Test Logging in Eclipse

[Logback](http://logback.qos.ch) really prefers to find a single `logback.xml` configuration file on the class path. There seems to be a subtle difference in how Eclipse and Maven build the class path used to run tests. Eclipse includes the test resources from all dependencies, whereas Surefire does not. This means that, if you run a test in `exec/java-exec`, such as `TestClassPathScanner`, the test resources from `common` will be on the class path if run from Eclipse, but not run from Surefire.

For the most part, having extra items on the class path in Eclipse is benign. Problems occur, however, when files of the same name appears multiple time on the class path. This is the situation with Logback.

When run under Maven Surefire, each project requires its own `logback.xml` (or, preferably `logback-test.xml`) in `src/test/resources`.

But, when run under Eclipse, the test program sees all the Logback config files on the path, causing Logback to print a bunch of log messages:

```
...WARN in ch.qos.logback.classic.LoggerContext[default] -
     Resource [logback.xml] occurs multiple times on the classpath.
...WARN in ch.qos.logback.classic.LoggerContext[default] -
     Resource [logback.xml] occurs at [file:/path/to/drill/exec/java-exec/target/test-classes/logback.xml]
...WARN in ch.qos.logback.classic.LoggerContext[default] -
     Resource [logback.xml] occurs at [file:/path/to/drill/common/target/test-classes/logback.xml]
```

To see this, add the following to any test from `java-exec`, such as `TestClassPathScanner`:
```
    System.out.println( "Class path: " + System.getProperty("java.class.path") );
```

When run in Eclipse:
```
Class path: /path/to/drill/exec/java-exec/target/test-classes:\
/path/to/drill/exec/java-exec/target/classes:...\
/path/to/drill/common/target/test-classes:...\
...INFO in ch.qos.logback.classic.LoggerContext[default] - \
Found resource [logback.xml] at [file:/path/to/drill/common/target/test-classes/logback.xml]
```
But, when run under Maven:
```
Class path: /path/to/drill/exec/java-exec/target/test-classes:\
/path/to/drill/exec/java-exec/target/classes:...\
/Users/me/.m2/repository/org/apache/drill/drill-common/1.9.0-SNAPSHOT/drill-common-1.9.0-SNAPSHOT.jar:...
...INFO in ch.qos.logback.classic.LoggerContext[default] - Could NOT find resource [logback.xml]
```

The Eclipse behavior seems to be both [known and unlikely to change](http://stackoverflow.com/a/15723127).

Based on the [Logback Configuration](http://logback.qos.ch/manual/configuration.html) documentation, the solution seems to be to apply a "No-op" logger by default. In Eclipse -> Preferences -> Java -> Installed JREs, select your default JRE or JDK. Click Edit... Add the following as one of the Default VM arguments:
```
-Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener
```
(Since this is a system property, it will be passed to every run or debug session. No harm if you happen to use a project that does not use Logback, the property will just be ignored by everything else.)

Then, if for some reason you want to see the Logback logging, add the following to your debug launch configuration:
```
-Dlogback.statusListenerClass=ch.qos.logback.core.status.OnConsoleStatusListener
```
The launch configuration option overrides (appears on the Java command line after) the global setting.

## Test Logging Configurations

### Default Test Log Levels

There is a global `logback-test.xml` configuration file in [common/src/test/resources/logback-test.xml](../../common/src/test/resources/logback-test.xml). This
logging configuration by default outputs error level logs to stdout.

Debug level logging to lilith can be turned on by adding `-Ddrill.lilith.enable=true` to the command used to run tests.

### Changing Test Log Levels

Often times it is most convenient to output logs to the console for debugging. This is best done programatically
by using the [LogFixture](../../exec/java-exec/src/test/java/org/apache/drill/test/LogFixture.java). The [LogFixture](../../exec/java-exec/src/test/java/org/apache/drill/test/LogFixture.java)
allows temporarily changing log levels for blocks of code programatically for debugging. An example of doing this is
the following.

```
    try(LogFixture logFixture = new LogFixture.LogFixtureBuilder()
      .logger(MyClass.class, Level.INFO)
      .toConsole() // This redirects output to stdout
      .build()) {
      // Code block with different log levels.
    }
```

More details on how to use the [LogFixture](../../exec/java-exec/src/test/java/org/apache/drill/test/LogFixture.java) can be found
int the javadocs for the class. Additionally, there are several methods that allow printing of query results to the console for debugging:

 * BaseTestQuery.printResult
 * QueryTestUtil.testRunAndPrint
 * QueryBuilder.print
 * ClusterTest.runAndPrint
 * ClientFixture.runQueriesAndPrint
 
**IMPORTANT NOTE:** The methods described above along with LogFixtureBuilder.toConsole() should only be used for debugging. Code
that uses these methods should not be committed, since it produces excess logging on our build servers.
