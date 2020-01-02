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
package org.apache.drill.cassandra;

import java.io.IOException;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.cassandra.CassandraStoragePlugin;
import org.apache.drill.exec.store.cassandra.CassandraStoragePluginConfig;
import org.apache.drill.exec.store.cassandra.connection.CassandraConnectionManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseCassandraTest extends BaseTestQuery implements CassandraTestConstants {
  private static final Logger logger = LoggerFactory.getLogger(BaseCassandraTest.class);

  private static final String CASSANDRA_STORAGE_PLUGIN_NAME = "cassandra";

  protected static CassandraStoragePlugin storagePlugin;

  protected static CassandraStoragePluginConfig storagePluginConfig;

  private static boolean testTablesCreated;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    storagePlugin = (CassandraStoragePlugin) pluginRegistry.getPlugin(CASSANDRA_STORAGE_PLUGIN_NAME);
    storagePluginConfig = storagePlugin.getConfig();
    storagePluginConfig.setEnabled(true);
    pluginRegistry.createOrUpdate(CASSANDRA_STORAGE_PLUGIN_NAME, storagePluginConfig, true);

    if (!testTablesCreated) {
      createTestCassandraTableIfNotExists(storagePluginConfig.getHosts(), storagePluginConfig.getPort());
      testTablesCreated = true;
    }
  }

  private static boolean createTestCassandraTableIfNotExists(List<String> host, int port) throws DrillException {
    try {
      logger.info("Initiating Cassandra Keyspace/Table for Test cases. Host: {}, Port: {}", host, port);
      Cluster cluster = CassandraConnectionManager.getCluster(host, port);
      Session session = cluster.connect();

      /* Create Schema: Keyspace */
      if (session.getCluster().getMetadata().getKeyspace(KEYSPACE_NAME) == null) {
        logger.info("Creating Keyspace: {}", KEYSPACE_NAME);
        session.execute("CREATE KEYSPACE " + KEYSPACE_NAME + " WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':3};");
      }

      /* Create Schema: Table */
      if (session.getCluster().getMetadata().getKeyspace(KEYSPACE_NAME).getTable(TABLE_NAME) == null) {
        logger.info("Creating Table [{}] in Keyspace [{}]", TABLE_NAME, KEYSPACE_NAME);
        session.execute("CREATE TABLE " + KEYSPACE_NAME + "." + TABLE_NAME + " (\n" + "  id text,\n" + "  pog_rank int,\n" + "  pog_id bigint,\n" + "  PRIMARY KEY (id, pog_rank)\n" + ");");

        /* Load Data */
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0001', 1, 10001);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0005', 1, 10001);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0002', 1, 10001);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0002', 2, 10001);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0002', 3, 10001);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0006', 1, 10001);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0006', 2, 10001);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0004', 1, 10001);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0004', 2, 10001);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0004', 3, 10002);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0004', 4, 10002);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0004', 5, 10002);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0004', 6, 10002);");
        session.execute("INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (id, pog_rank, pog_id) VALUES ('id0003', 1, 10001);");
      }
      return true;
    } catch (Exception e) {
      throw new DrillException("Failure while Cassandra Test table creation", e);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

  }

  protected String getPlanText(String planFile, String keyspaceName, String tableName) throws IOException {
    return Files.toString(DrillFileUtils.getResourceAsFile(planFile), Charsets.UTF_8).replace("[TABLE_NAME]", tableName).replace("[KEYSPACE_NAME]", keyspaceName);
  }

  protected void runCassandraPhysicalVerifyCount(String planFile, String keyspaceName, String tableName, int expectedRowCount) throws Exception {
    String physicalPlan = getPlanText(planFile, keyspaceName, tableName);
    List<QueryDataBatch> results = testPhysicalWithResults(physicalPlan);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  protected List<QueryDataBatch> runCassandraSQLlWithResults(String sql) throws Exception {
    System.out.println("Running query:\n" + sql);
    return testSqlWithResults(sql);
  }

  protected void runCassandraSQLVerifyCount(String sql, int expectedRowCount) throws Exception {
    List<QueryDataBatch> results = runCassandraSQLlWithResults(sql);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  private void printResultAndVerifyRowCount(List<QueryDataBatch> results, int expectedRowCount) throws SchemaChangeException {
    int rowCount;
    rowCount = printResult(results);
    if (expectedRowCount != -1) {
      Assert.assertEquals(expectedRowCount, rowCount);
    }
  }
}
