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
package org.apache.drill.exec.impersonation;

import com.google.common.collect.Maps;
import org.apache.drill.categories.SecurityTest;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.categories.SlowTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Note to future devs, please do not put random tests here. Make sure that they actually require
 * access to a DFS instead of the local filesystem implementation used by default in the rest of
 * the tests. Running this mini cluster is slow and it is best for these tests to only cover
 * necessary cases.
 *
 * <p><b>IMPORTANT: These tests are currently disabled due to Jetty version conflicts.</b></p>
 *
 * <h3>Why These Tests Are Disabled:</h3>
 * <p>
 * Apache Drill has been upgraded to use Jetty 12 (with Jakarta EE 10 APIs) to address security
 * vulnerabilities and maintain compatibility with modern Java versions. However, Apache Hadoop
 * 3.x (currently 3.4.1) still depends on Jetty 9, which uses the older javax.servlet APIs.
 * </p>
 *
 * <p>
 * When tests attempt to start both:
 * <ul>
 *   <li>Drill's embedded web server (Jetty 12)</li>
 *   <li>Hadoop's MiniDFSCluster (Jetty 9)</li>
 * </ul>
 * The conflicting Jetty versions on the classpath cause {@code NoClassDefFoundError} exceptions,
 * as Jetty 12 refactored many core classes (e.g., {@code org.eclipse.jetty.server.Request$Handler}
 * is a new Jetty 12 interface that doesn't exist in Jetty 9).
 * </p>
 *
 * <h3>Attempted Solutions:</h3>
 * <ol>
 *   <li><b>Disabling Drill's HTTP server:</b> Failed because drill-java-exec classes were compiled
 *       against Jetty 12, and the bytecode contains hard references to Jetty 12 classes that fail
 *       to load even when the HTTP server is disabled.</li>
 *   <li><b>Excluding Jetty from dependencies:</b> Failed due to Maven's inability to have two
 *       different versions of the same artifact (org.eclipse.jetty:*) on the classpath
 *       simultaneously.</li>
 *   <li><b>Separate test module with Jetty 9:</b> Failed because depending on drill-java-exec
 *       JAR (compiled with Jetty 12) brings Jetty 12 class references into the test classpath.</li>
 * </ol>
 *
 * <h3>When Will These Tests Be Re-enabled:</h3>
 * <p>
 * These tests will be re-enabled when one of the following occurs:
 * <ul>
 *   <li>Apache Hadoop 4.x is released with Jetty 12 support</li>
 *   <li>A Hadoop 3.x maintenance release upgrades to Jetty 12 (tracked in
 *       <a href="https://issues.apache.org/jira/browse/HADOOP-19625">HADOOP-19625</a>)</li>
 *   <li>Drill implements a separate test harness that recompiles necessary classes against Jetty 9</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>Note:</b> HADOOP-19625 is currently open and targets Jetty 12 EE10, but requires Java 17 as
 * the baseline (tracked in HADOOP-17177). No specific Hadoop release version or timeline has been
 * announced yet.
 * </p>
 *
 * <h3>Testing Alternatives:</h3>
 * <p>
 * HDFS impersonation functionality can still be tested using:
 * <ul>
 *   <li>Integration tests against a real Hadoop cluster</li>
 *   <li>Manual testing with HDFS-enabled environments</li>
 *   <li>Tests that use local filesystem instead of MiniDFSCluster (see other impersonation tests)</li>
 * </ul>
 * </p>
 *
 * @see <a href="https://issues.apache.org/jira/browse/DRILL-XXXX">DRILL-XXXX: Jetty 12 Migration</a>
 */
@Ignore("Disabled due to Jetty 9/12 version conflict with Hadoop MiniDFSCluster - see class javadoc for details")
@Category({SlowTest.class, SecurityTest.class})
public class TestImpersonationDisabledWithMiniDFS extends BaseTestImpersonation {

  @BeforeClass
  public static void setup() throws Exception {
    startMiniDfsCluster(TestImpersonationDisabledWithMiniDFS.class.getSimpleName(), false);
    startDrillCluster(false);
    addMiniDfsBasedStorage(Maps.<String, WorkspaceConfig>newHashMap());
    createTestData();
  }

  private static void createTestData() throws Exception {
    // Create test table in minidfs.tmp schema for use in test queries
    run(String.format("CREATE TABLE %s.tmp.dfsRegion AS SELECT * FROM cp.`region.json`", MINI_DFS_STORAGE_PLUGIN_NAME));

    // generate a large enough file that the DFS will not fulfill requests to read a
    // page of data all at once, see notes above testReadLargeParquetFileFromDFS()
    run(String.format(
        "CREATE TABLE %s.tmp.large_employee AS " +
            "(SELECT employee_id, full_name FROM cp.`employee.json`) " +
            "UNION ALL (SELECT employee_id, full_name FROM cp.`employee.json`)" +
            "UNION ALL (SELECT employee_id, full_name FROM cp.`employee.json`)" +
            "UNION ALL (SELECT employee_id, full_name FROM cp.`employee.json`)" +
            "UNION ALL (SELECT employee_id, full_name FROM cp.`employee.json`)" +
            "UNION ALL (SELECT employee_id, full_name FROM cp.`employee.json`)" +
            "UNION ALL (SELECT employee_id, full_name FROM cp.`employee.json`)" +
        "UNION ALL (SELECT employee_id, full_name FROM cp.`employee.json`)", MINI_DFS_STORAGE_PLUGIN_NAME));
  }

  /**
   * When working on merging the Drill fork of parquet a bug was found that only manifested when
   * run on a cluster. It appears that the local implementation of the Hadoop FileSystem API
   * never fails to provide all of the bytes that are requested in a single read. The API is
   * designed to allow for a subset of the requested bytes be returned, and a client can decide
   * if they want to do processing on teh subset that are available now before requesting the rest.
   *
   * For parquet's block compression of page data, we need all of the bytes. This test is here as
   * a sanitycheck  to make sure we don't accidentally introduce an issue where a subset of the bytes
   * are read and would otherwise require testing on a cluster for the full contract of the read method
   * we are using to be exercised.
   */
  @Test
  public void testReadLargeParquetFileFromDFS() throws Exception {
    run(String.format("USE %s", MINI_DFS_STORAGE_PLUGIN_NAME));
    run("SELECT * FROM tmp.`large_employee`");
  }

  @Test // DRILL-3037
  public void testSimpleQuery() throws Exception {
    final String query =
        String.format("SELECT sales_city, sales_country FROM tmp.dfsRegion ORDER BY region_id DESC LIMIT 2");

    testBuilder()
        .optionSettingQueriesForTestQuery(String.format("USE %s", MINI_DFS_STORAGE_PLUGIN_NAME))
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("sales_city", "sales_country")
        .baselineValues("Santa Fe", "Mexico")
        .baselineValues("Santa Anita", "Mexico")
        .go();
  }

  @AfterClass
  public static void removeMiniDfsBasedStorage() throws Exception {
    cluster.storageRegistry().remove(MINI_DFS_STORAGE_PLUGIN_NAME);
    stopMiniDfsCluster();
  }
}
