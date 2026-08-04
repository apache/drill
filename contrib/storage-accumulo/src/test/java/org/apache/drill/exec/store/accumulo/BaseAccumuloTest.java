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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryRowSetIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for Accumulo integration tests.
 *
 * <p>This class sets up the Drill test cluster and registers the Accumulo storage plugin
 * configured to connect to the MiniAccumuloCluster.</p>
 */
public class BaseAccumuloTest extends ClusterTest {

  public static final String ACCUMULO_STORAGE_PLUGIN_NAME = "accumulo";

  protected static AccumuloStoragePlugin storagePlugin;
  protected static AccumuloStoragePluginConfig storagePluginConfig;

  @BeforeClass
  public static void setupAccumuloTestCluster() throws Exception {
    // Initialize the MiniAccumuloCluster
    boolean isManaged = Boolean.parseBoolean(System.getProperty("drill.accumulo.tests.managed", "true"));
    AccumuloIntegrationTestsSuite.configure(isManaged, true);
    AccumuloIntegrationTestsSuite.initCluster();

    // Start the Drill test cluster
    startCluster(ClusterFixture.builder(dirTestWatcher));

    // Register Accumulo storage plugin
    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
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
  public static void tearDownAccumuloTestCluster() throws Exception {
    AccumuloIntegrationTestsSuite.tearDownCluster();
  }

  /**
   * Runs a SQL query and verifies the row count. Pass {@code -1} to skip the check.
   */
  protected void runAccumuloSQLVerifyCount(String sql, int expectedRowCount) throws Exception {
    long rowCount = queryBuilder().sql(sql).run().recordCount();
    if (expectedRowCount != -1) {
      assertEquals(expectedRowCount, rowCount);
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

  /**
   * Returns a {@code FROM} clause that aliases the table as {@code t}. Referring to a
   * qualifier inside a column family requires the table alias ({@code t.cf.name}),
   * the same as for the HBase plugin.
   */
  protected String fromTable(String tableName) {
    return " FROM " + fullTableName(tableName) + " t";
  }

  /**
   * Wraps a column reference in a {@code CONVERT_FROM(..., 'UTF8')} call. Accumulo row
   * keys and values are surfaced to Drill as VARBINARY, so they must be decoded before
   * they can be compared against string baselines.
   *
   * @param column the column reference, e.g. {@code row_key} or {@code cf.name}
   * @param alias the alias to give the decoded column
   */
  protected static String utf8(String column, String alias) {
    return "CONVERT_FROM(" + column + ", 'UTF8') AS " + alias;
  }

  /**
   * Runs a query and returns its results as rows of strings, with {@code null} for
   * NULL values. Reading the values back as strings keeps the assertions independent
   * of whether a column comes back as required or nullable.
   */
  protected List<List<String>> runAndReadStrings(String sql) throws Exception {
    return readStrings(queryBuilder().sql(sql).rowSetIterator());
  }

  /**
   * Drains a query's row sets into rows of strings, with {@code null} for NULL values.
   *
   * <p>Every batch is read, so this stays correct for queries that return their results
   * across several batches, and each row set is released as it is consumed.</p>
   */
  protected static List<List<String>> readStrings(QueryRowSetIterator batches) {
    List<List<String>> rows = new ArrayList<>();
    for (DirectRowSet rowSet : batches) {
      try {
        int columnCount = rowSet.schema().size();
        RowSetReader reader = rowSet.reader();
        while (reader.next()) {
          List<String> row = new ArrayList<>(columnCount);
          for (int i = 0; i < columnCount; i++) {
            ScalarReader scalar = reader.scalar(i);
            row.add(scalar.isNull() ? null : String.valueOf(scalar.getObject()));
          }
          rows.add(row);
        }
      } finally {
        rowSet.clear();
      }
    }
    return rows;
  }
}
