/**
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
package org.apache.drill.exec.store.mongo;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

public class MongoTestBase extends PlanTestBase implements MongoTestConstants {
  protected static MongoStoragePlugin storagePlugin;
  protected static MongoStoragePluginConfig storagePluginConfig;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initMongoStoragePlugin();
  }

  public static void initMongoStoragePlugin() throws Exception {
    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    storagePlugin = (MongoStoragePlugin) pluginRegistry.getPlugin(MongoStoragePluginConfig.NAME);
    storagePluginConfig = storagePlugin.getConfig();
    storagePluginConfig.setEnabled(true);
    pluginRegistry.createOrUpdate(MongoStoragePluginConfig.NAME, storagePluginConfig, true);
    if (System.getProperty("drill.mongo.tests.bson.reader", "true").equalsIgnoreCase("false")) {
      testNoResult(String.format("alter session set `%s` = false", ExecConstants.MONGO_BSON_RECORD_READER));
    } else {
      testNoResult(String.format("alter session set `%s` = true", ExecConstants.MONGO_BSON_RECORD_READER));
    }
  }

  public List<QueryDataBatch> runMongoSQLWithResults(String sql)
      throws Exception {
    return testSqlWithResults(sql);
  }

  public void runMongoSQLVerifyCount(String sql, int expectedRowCount)
      throws Exception {
    List<QueryDataBatch> results = runMongoSQLWithResults(sql);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  public void printResultAndVerifyRowCount(List<QueryDataBatch> results,
      int expectedRowCount) throws SchemaChangeException {
    int rowCount = printResult(results);
    if (expectedRowCount != -1) {
      Assert.assertEquals(expectedRowCount, rowCount);
    }
  }

  public void testHelper(String query, String expectedExprInPlan,
      int expectedRecordCount) throws Exception {
    testPhysicalPlan(query, expectedExprInPlan);
    int actualRecordCount = testSql(query);
    assertEquals(
        String.format(
            "Received unexpected number of rows in output: expected=%d, received=%s",
            expectedRecordCount, actualRecordCount), expectedRecordCount,
        actualRecordCount);
  }

  @AfterClass
  public static void tearDownMongoTestBase() throws Exception {
    storagePlugin = null;
  }

}