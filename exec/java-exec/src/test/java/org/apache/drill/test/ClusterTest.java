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
 * limitations under the License.‰
 */
package org.apache.drill.test;

import java.io.IOException;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.test.rowSet.RowSet;
import org.junit.AfterClass;
import org.junit.ClassRule;

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

  public void test(String sqlQuery) throws Exception {
    client.runQueries(sqlQuery);
  }

  public static void test(String query, Object... args) throws Exception {
    client.queryBuilder().sql(query, args).run( );
  }

  public QueryBuilder queryBuilder( ) {
    return client.queryBuilder();
  }

  /**
   * Handy development-time tool to run a query and print the results. Use this
   * when first developing tests. Then, encode the expected results using
   * the appropriate tool and verify them rather than just printing them to
   * create the final test.
   *
   * @param sql the query to run
   */

  protected void runAndPrint(String sql) {
    QueryResultSet results = client.queryBuilder().sql(sql).resultSet();
    try {
      for (;;) {
        RowSet rowSet = results.next();
        if (rowSet == null) {
          break;
        }
        if (rowSet.rowCount() > 0) {
          rowSet.print();
        }
        rowSet.clear();
      }
      System.out.println(results.recordCount());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    } finally {
      results.close();
    }
  }
}
