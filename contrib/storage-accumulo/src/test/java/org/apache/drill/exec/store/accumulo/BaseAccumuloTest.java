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
package org.apache.drill.exec.store.accumulo;

import java.util.List;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.BaseTestQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

/**
 * Base class for Accumulo integration tests.
 *
 * <p>This class sets up the Drill test cluster and registers the Accumulo storage plugin
 * configured to connect to the MiniAccumuloCluster.</p>
 */
public class BaseAccumuloTest extends BaseTestQuery {

  public static final String ACCUMULO_STORAGE_PLUGIN_NAME = "accumulo";

  protected static AccumuloStoragePlugin storagePlugin;
  protected static AccumuloStoragePluginConfig storagePluginConfig;

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    // Initialize the MiniAccumuloCluster
    boolean isManaged = Boolean.parseBoolean(System.getProperty("drill.accumulo.tests.managed", "true"));
    AccumuloIntegrationTestsSuite.configure(isManaged, true);
    AccumuloIntegrationTestsSuite.initCluster();

    // Start the Drill test cluster
    BaseTestQuery.setupDefaultTestCluster();

    // Register Accumulo storage plugin
    StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    storagePluginConfig = new AccumuloStoragePluginConfig(
        AccumuloIntegrationTestsSuite.getZooKeepers(),
        AccumuloIntegrationTestsSuite.getInstanceName(),
        AccumuloIntegrationTestsSuite.getRootUser(),
        AccumuloIntegrationTestsSuite.getRootPassword()
    );
    storagePluginConfig.setEnabled(true);

    pluginRegistry.put(ACCUMULO_STORAGE_PLUGIN_NAME, storagePluginConfig);
    storagePlugin = (AccumuloStoragePlugin) pluginRegistry.getPlugin(ACCUMULO_STORAGE_PLUGIN_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AccumuloIntegrationTestsSuite.tearDownCluster();
  }

  /**
   * Runs a SQL query and verifies the row count.
   */
  protected void runAccumuloSQLVerifyCount(String sql, int expectedRowCount) throws Exception {
    List<QueryDataBatch> results = testSqlWithResults(sql);
    logResultAndVerifyRowCount(results, expectedRowCount);
  }

  /**
   * Runs a SQL query and returns the results for inspection.
   */
  protected List<QueryDataBatch> runAccumuloSQLWithResults(String sql) throws Exception {
    return testSqlWithResults(sql);
  }

  /**
   * Logs the results and verifies the row count.
   */
  private void logResultAndVerifyRowCount(List<QueryDataBatch> results, int expectedRowCount)
      throws SchemaChangeException {
    int rowCount = logResult(results);
    if (expectedRowCount != -1) {
      Assert.assertEquals(expectedRowCount, rowCount);
    }
  }

  /**
   * Returns the fully qualified table name for Drill queries.
   *
   * @param tableName the Accumulo table name
   * @return the fully qualified name like "accumulo.`tableName`"
   */
  protected String fullTableName(String tableName) {
    return ACCUMULO_STORAGE_PLUGIN_NAME + ".`" + tableName + "`";
  }
}
