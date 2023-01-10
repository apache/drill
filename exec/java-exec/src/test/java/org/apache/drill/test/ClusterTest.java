/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.test;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.UserException;
import org.junit.AfterClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.fail;

/**
 * Base class for tests that use a single cluster fixture for a set of
 * tests. Extend your test case directly from {@link DrillTest} if you
 * need to start up and shut down a cluster multiple times.
 * <p>
 * To create a test with a single cluster config, do the following:
 * <pre><code>
 * public class YourTest extends ClusterTest {
 *   {@literal @}BeforeClass
 *   public static setup( ) throws Exception {
 *     FixtureBuilder builder = ClusterFixture.builder()
 *       // Set options, etc.
 *       ;
 *     startCluster(builder);
 *   }
 *
 *   // Your tests
 * }
 * </code></pre>
 * This class takes care of shutting down the cluster at the end of the test.
 * <p>
 * The simplest possible setup:
 * <pre><code>
 *   {@literal @}BeforeClass
 *   public static setup( ) throws Exception {
 *     startCluster(ClusterFixture.builder( ));
 *   }
 * </code></pre>
 * <p>
 * If you need to start the cluster with different (boot time) configurations,
 * do the following instead:
 * <pre><code>
 * public class YourTest extends DrillTest {
 *   {@literal @}Test
 *   public someTest() throws Exception {
 *     FixtureBuilder builder = ClusterFixture.builder()
 *       // Set options, etc.
 *       ;
 *     try(ClusterFixture cluster = builder.build) {
 *       // Tests here
 *     }
 *   }
 * }
 * </code></pre>
 * The try-with-resources block ensures that the cluster is shut down at
 * the end of each test method.
 */
public class ClusterTest extends DrillTest {

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  protected static ClusterFixture cluster;
  protected static ClientFixture client;

  protected static void startCluster(ClusterFixtureBuilder builder) throws Exception {
    cluster = builder.build();
    client = cluster.clientFixture();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    AutoCloseables.close(client, cluster);
  }

  /**
   * Convenience method when converting classic tests to use the
   * cluster fixture.
   * @return a test builder that works against the cluster fixture
   */
  public TestBuilder testBuilder() {
    return client.testBuilder();
  }

  /**
   * Convenience method when converting classic tests to use the
   * cluster fixture.
   * @return the contents of the resource text file
   */
  public String getFile(String resource) throws IOException {
    return ClusterFixture.getResource(resource);
  }

  public void runAndLog(String sqlQuery) {
    client.runQueriesAndLog(sqlQuery);
  }

  public void runAndPrint(String sqlQuery) {
    client.runQueriesAndPrint(sqlQuery);
  }

  public void runAndPrint(String sqlQuery, Object... args) {
    runAndPrint(String.format(sqlQuery, args));
  }

  public static void run(String query, Object... args) throws Exception {
    client.queryBuilder().sql(query, args).run();
  }

  public QueryBuilder queryBuilder( ) {
    return client.queryBuilder();
  }

  /**
   * Utility method which tests given query produces a {@link UserException} and the exception message contains
   * the given message.
   * @param testSqlQuery Test query
   * @param expectedErrorMsg Expected error message.
   */
  protected static void errorMsgTestHelper(String testSqlQuery, String expectedErrorMsg) throws Exception {
    try {
      run(testSqlQuery);
      fail("Expected a UserException when running " + testSqlQuery);
    } catch (UserException actualException) {
      try {
        assertThat("message of UserException when running " + testSqlQuery, actualException.getMessage(), containsString(expectedErrorMsg));
      } catch (AssertionError e) {
        e.addSuppressed(actualException);
        throw e;
      }
    }
  }

  protected static void updateClient(Properties properties) {
    if (client != null) {
      client.close();
      client = null;
    }
    ClientFixture.ClientBuilder clientBuilder = cluster.clientBuilder();
    if (properties != null) {
      for (final String key : properties.stringPropertyNames()) {
        final String lowerCaseKey = key.toLowerCase();
        clientBuilder.property(lowerCaseKey, properties.getProperty(key));
      }
    }
    client = clientBuilder.build();
  }

  protected static void updateClient(final String user) {
    updateClient(user, null);
  }

  protected static void updateClient(final String user, final String password) {
    if (client != null) {
      client.close();
      client = null;
    }
    final Properties properties = new Properties();
    properties.setProperty(DrillProperties.USER, user);
    if (password != null) {
      properties.setProperty(DrillProperties.PASSWORD, password);
    }
    updateClient(properties);
  }
}
