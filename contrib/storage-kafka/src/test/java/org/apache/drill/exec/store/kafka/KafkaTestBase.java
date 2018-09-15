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
package org.apache.drill.exec.store.kafka;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.kafka.cluster.EmbeddedKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;

import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

public class KafkaTestBase extends PlanTestBase {
  protected static KafkaStoragePluginConfig storagePluginConfig;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Make sure this test is only running as part of the suit
    Assume.assumeTrue(TestKafkaSuit.isRunningSuite());
    TestKafkaSuit.initKafka();
    initKafkaStoragePlugin(TestKafkaSuit.embeddedKafkaCluster);
  }

  public static void initKafkaStoragePlugin(EmbeddedKafkaCluster embeddedKafkaCluster) throws Exception {
    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    Map<String, String> kafkaConsumerProps = Maps.newHashMap();
    kafkaConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaCluster.getKafkaBrokerList());
    kafkaConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "drill-test-consumer");
    storagePluginConfig = new KafkaStoragePluginConfig(kafkaConsumerProps);
    storagePluginConfig.setEnabled(true);
    pluginRegistry.createOrUpdate(KafkaStoragePluginConfig.NAME, storagePluginConfig, true);
    testNoResult(String.format("alter session set `%s` = '%s'", ExecConstants.KAFKA_RECORD_READER,
        "org.apache.drill.exec.store.kafka.decoders.JsonMessageReader"));
    testNoResult(String.format("alter session set `%s` = %d", ExecConstants.KAFKA_POLL_TIMEOUT, 5000));
  }

  public List<QueryDataBatch> runKafkaSQLWithResults(String sql) throws Exception {
    return testSqlWithResults(sql);
  }

  public void runKafkaSQLVerifyCount(String sql, int expectedRowCount) throws Exception {
    List<QueryDataBatch> results = runKafkaSQLWithResults(sql);
    logResultAndVerifyRowCount(results, expectedRowCount);
  }

  public void logResultAndVerifyRowCount(List<QueryDataBatch> results, int expectedRowCount)
      throws SchemaChangeException {
    int rowCount = logResult(results);
    if (expectedRowCount != -1) {
      Assert.assertEquals(expectedRowCount, rowCount);
    }
  }

  public void testHelper(String query, String expectedExprInPlan, int expectedRecordCount) throws Exception {
    testPhysicalPlan(query, expectedExprInPlan);
    int actualRecordCount = testSql(query);
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @AfterClass
  public static void tearDownKafkaTestBase() throws Exception {
    if (TestKafkaSuit.isRunningSuite()) {
      TestKafkaSuit.tearDownCluster();
    }
  }

}
