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

import org.apache.drill.categories.KafkaStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.store.kafka.TestKafkaSuit.embeddedKafkaCluster;
import static org.junit.Assert.fail;

@Category({KafkaStorageTest.class, SlowTest.class})
public class KafkaQueriesTest extends KafkaTestBase {

  @Test
  public void testSqlQueryOnInvalidTopic() throws Exception {
    String queryString = String.format(TestQueryConstants.MSG_SELECT_QUERY, TestQueryConstants.INVALID_TOPIC);
    try {
      testBuilder()
        .sqlQuery(queryString)
        .unOrdered()
        .baselineRecords(Collections.emptyList())
        .go();
      fail("Test passed though topic does not exist.");
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
    testBuilder()
      .sqlQuery(queryString)
      .unOrdered()
      .baselineColumns("minOffset")
      .baselineValues(startOffsetsMap.get(new TopicPartition(TestQueryConstants.JSON_TOPIC, 0)))
      .go();
  }

  @Test
  public void testPartitionMaxOffset() throws Exception {
    // following kafka.tools.GetOffsetShell for latest as -1
    Map<TopicPartition, Long> endOffsetsMap = fetchOffsets(-1);

    String queryString = String.format(TestQueryConstants.MAX_OFFSET_QUERY, TestQueryConstants.JSON_TOPIC);
    testBuilder()
      .sqlQuery(queryString)
      .unOrdered()
      .baselineColumns("maxOffset")
      .baselineValues(endOffsetsMap.get(new TopicPartition(TestQueryConstants.JSON_TOPIC, 0)) - 1)
      .go();
  }

  @Test
  public void testInformationSchema() throws Exception {
    String query = "select * from information_schema.`views`";
    runSQL(query);
  }

  private Map<TopicPartition, Long> fetchOffsets(int flag) {
    Consumer<byte[], byte[]> kafkaConsumer = null;
    try {
     kafkaConsumer = new KafkaConsumer<>(storagePluginConfig.getKafkaConsumerProps(),
        new ByteArrayDeserializer(), new ByteArrayDeserializer());

      Map<TopicPartition, Long> offsetsMap = new HashMap<>();
      kafkaConsumer.subscribe(Collections.singletonList(TestQueryConstants.JSON_TOPIC));
      // based on KafkaConsumer JavaDoc, seekToBeginning/seekToEnd functions
      // evaluates lazily, seeking to the
      // first/last offset in all partitions only when poll(long) or
      // position(TopicPartition) are called
      kafkaConsumer.poll(0);
      Set<TopicPartition> assignments = kafkaConsumer.assignment();

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
      return offsetsMap;
    } finally {
      embeddedKafkaCluster.registerToClose(kafkaConsumer);
    }
  }

  @Test
  public void testPhysicalPlanSubmission() throws Exception {
    String query = String.format(TestQueryConstants.MSG_SELECT_QUERY, TestQueryConstants.JSON_TOPIC);
    testPhysicalPlanExecutionBasedOnQuery(query);
  }

  @Test
  public void testOneMessageTopic() throws Exception {
    String topicName = "topicWithOneMessage";
    TestKafkaSuit.createTopicHelper(topicName, 1);
    KafkaMessageGenerator generator = new KafkaMessageGenerator(embeddedKafkaCluster.getKafkaBrokerList(), StringSerializer.class);
    generator.populateMessages(topicName, "{\"index\": 1}");

    testBuilder()
      .sqlQuery("select index from kafka.`%s`", topicName)
      .unOrdered()
      .baselineColumns("index")
      .baselineValues(1L)
      .go();
  }

  @Test
  public void testMalformedRecords() throws Exception {
    String topicName = "topicWithMalFormedMessages";
    TestKafkaSuit.createTopicHelper(topicName, 1);
    try {
      KafkaMessageGenerator generator = new KafkaMessageGenerator(embeddedKafkaCluster.getKafkaBrokerList(), StringSerializer.class);
      generator.populateMessages(topicName, "Test");

      alterSession(ExecConstants.KAFKA_READER_SKIP_INVALID_RECORDS, false);
      try {
        test("select * from kafka.`%s`", topicName);
        fail();
      } catch (UserException e) {
        // expected
      }

      alterSession(ExecConstants.KAFKA_READER_SKIP_INVALID_RECORDS, true);
      testBuilder()
        .sqlQuery("select * from kafka.`%s`", topicName)
        .expectsEmptyResultSet();

      generator.populateMessages(topicName, "{\"index\": 1}", "", "   ", "{Invalid}", "{\"index\": 2}");

      testBuilder()
        .sqlQuery("select index from kafka.`%s`", topicName)
        .unOrdered()
        .baselineColumns("index")
        .baselineValues(1L)
        .baselineValues(2L)
        .go();
    } finally {
      resetSessionOption(ExecConstants.KAFKA_READER_SKIP_INVALID_RECORDS);
    }
  }

  @Test
  public void testNanInf() throws Exception {
    String topicName = "topicWithNanInf";
    TestKafkaSuit.createTopicHelper(topicName, 1);
    try {
      KafkaMessageGenerator generator = new KafkaMessageGenerator(embeddedKafkaCluster.getKafkaBrokerList(), StringSerializer.class);
      generator.populateMessages(topicName, "{\"nan_col\":NaN, \"inf_col\":Infinity}");

      alterSession(ExecConstants.KAFKA_READER_NAN_INF_NUMBERS, false);
      try {
        test("select nan_col, inf_col from kafka.`%s`", topicName);
        fail();
      } catch (UserException e) {
        // expected
      }

      alterSession(ExecConstants.KAFKA_READER_NAN_INF_NUMBERS, true);
      testBuilder()
        .sqlQuery("select nan_col, inf_col from kafka.`%s`", topicName)
        .unOrdered()
        .baselineColumns("nan_col", "inf_col")
        .baselineValues(Double.NaN, Double.POSITIVE_INFINITY)
        .go();
    } finally {
      resetSessionOption(ExecConstants.KAFKA_READER_NAN_INF_NUMBERS);
    }
  }

  @Test
  public void testEscapeAnyChar() throws Exception {
    String topicName = "topicWithEscapeAnyChar";
    TestKafkaSuit.createTopicHelper(topicName, 1);
    try {
      KafkaMessageGenerator generator = new KafkaMessageGenerator(embeddedKafkaCluster.getKafkaBrokerList(), StringSerializer.class);
      generator.populateMessages(topicName, "{\"name\": \"AB\\\"\\C\"}");

      alterSession(ExecConstants.KAFKA_READER_ESCAPE_ANY_CHAR, false);
      try {
        test("select name from kafka.`%s`", topicName);
        fail();
      } catch (UserException e) {
        // expected
      }

      alterSession(ExecConstants.KAFKA_READER_ESCAPE_ANY_CHAR, true);
      testBuilder()
        .sqlQuery("select name from kafka.`%s`", topicName)
        .unOrdered()
        .baselineColumns("name")
        .baselineValues("AB\"C")
        .go();
    } finally {
      resetSessionOption(ExecConstants.KAFKA_READER_ESCAPE_ANY_CHAR);
    }
  }
}
