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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.drill.categories.KafkaStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Assert;
import org.junit.Test;

import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.junit.experimental.categories.Category;

@Category({KafkaStorageTest.class, SlowTest.class})
public class KafkaQueriesTest extends KafkaTestBase {

  @Test
  public void testSqlQueryOnInvalidTopic() throws Exception {
    String queryString = String.format(TestQueryConstants.MSG_SELECT_QUERY, TestQueryConstants.INVALID_TOPIC);
    try {
      testBuilder().sqlQuery(queryString).unOrdered().baselineRecords(Collections.<Map<String, Object>> emptyList())
          .build().run();
      Assert.fail("Test passed though topic does not exist.");
    } catch (RpcException re) {
      Assert.assertTrue(re.getMessage().contains("DATA_READ ERROR: Table 'invalid-topic' does not exist"));
    }
  }

  @Test
  public void testResultCount() throws Exception {
    String queryString = String.format(TestQueryConstants.MSG_SELECT_QUERY, TestQueryConstants.JSON_TOPIC);
    runKafkaSQLVerifyCount(queryString, TestKafkaSuit.NUM_JSON_MSG);
  }

  @Test
  public void testPartitionMinOffset() throws Exception {
    // following kafka.tools.GetOffsetShell for earliest as -2
    Map<TopicPartition, Long> startOffsetsMap = fetchOffsets(-2);

    String queryString = String.format(TestQueryConstants.MIN_OFFSET_QUERY, TestQueryConstants.JSON_TOPIC);
    testBuilder().sqlQuery(queryString).unOrdered().baselineColumns("minOffset")
        .baselineValues(startOffsetsMap.get(new TopicPartition(TestQueryConstants.JSON_TOPIC, 0))).go();
  }

  @Test
  public void testPartitionMaxOffset() throws Exception {
    // following kafka.tools.GetOffsetShell for latest as -1
    Map<TopicPartition, Long> endOffsetsMap = fetchOffsets(-1);

    String queryString = String.format(TestQueryConstants.MAX_OFFSET_QUERY, TestQueryConstants.JSON_TOPIC);
    testBuilder().sqlQuery(queryString).unOrdered().baselineColumns("maxOffset")
        .baselineValues(endOffsetsMap.get(new TopicPartition(TestQueryConstants.JSON_TOPIC, 0))-1).go();
  }

  @Test
  public void testInformationSchema() throws Exception {
    String query = "select * from information_schema.`views`";
    runSQL(query);
  }

  private Map<TopicPartition, Long> fetchOffsets(int flag) {
    KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(storagePluginConfig.getKafkaConsumerProps(),
        new ByteArrayDeserializer(), new ByteArrayDeserializer());

    Map<TopicPartition, Long> offsetsMap = Maps.newHashMap();
    kafkaConsumer.subscribe(Collections.singletonList(TestQueryConstants.JSON_TOPIC));
    // based on KafkaConsumer JavaDoc, seekToBeginning/seekToEnd functions
    // evaluates lazily, seeking to the
    // first/last offset in all partitions only when poll(long) or
    // position(TopicPartition) are called
    kafkaConsumer.poll(0);
    Set<TopicPartition> assignments = kafkaConsumer.assignment();

    try {
      if (flag == -2) {
        // fetch start offsets for each topicPartition
        kafkaConsumer.seekToBeginning(assignments);
        for (TopicPartition topicPartition : assignments) {
          offsetsMap.put(topicPartition, kafkaConsumer.position(topicPartition));
        }
      } else if (flag == -1) {
        // fetch end offsets for each topicPartition
        kafkaConsumer.seekToEnd(assignments);
        for (TopicPartition topicPartition : assignments) {
          offsetsMap.put(topicPartition, kafkaConsumer.position(topicPartition));
        }
      } else {
        throw new RuntimeException(String.format("Unsupported flag %d", flag));
      }
    } finally {
      kafkaConsumer.close();
    }
    return offsetsMap;
  }

  @Test
  public void testPhysicalPlanSubmission() throws Exception {
    String query = String.format(TestQueryConstants.MSG_SELECT_QUERY, TestQueryConstants.JSON_TOPIC);
    testPhysicalPlanExecutionBasedOnQuery(query);
  }

}
