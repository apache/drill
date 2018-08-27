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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import kafka.common.KafkaException;

public class MessageIterator implements Iterator<ConsumerRecord<byte[], byte[]>> {

  private static final Logger logger = LoggerFactory.getLogger(MessageIterator.class);
  private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
  private Iterator<ConsumerRecord<byte[], byte[]>> recordIter;
  private final TopicPartition topicPartition;
  private long totalFetchTime = 0;
  private final long kafkaPollTimeOut;
  private final long endOffset;

  public MessageIterator(final KafkaConsumer<byte[], byte[]> kafkaConsumer, final KafkaPartitionScanSpec subScanSpec,
      final long kafkaPollTimeOut) {
    this.kafkaConsumer = kafkaConsumer;
    this.kafkaPollTimeOut = kafkaPollTimeOut;

    List<TopicPartition> partitions = Lists.newArrayListWithCapacity(1);
    topicPartition = new TopicPartition(subScanSpec.getTopicName(), subScanSpec.getPartitionId());
    partitions.add(topicPartition);
    this.kafkaConsumer.assign(partitions);
    logger.info("Start offset of {}:{} is - {}", subScanSpec.getTopicName(), subScanSpec.getPartitionId(),
        subScanSpec.getStartOffset());
    this.kafkaConsumer.seek(topicPartition, subScanSpec.getStartOffset());
    this.endOffset = subScanSpec.getEndOffset();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Does not support remove operation");
  }

  @Override
  public boolean hasNext() {
    if (recordIter != null && recordIter.hasNext()) {
      return true;
    }

    long nextPosition = kafkaConsumer.position(topicPartition);
    if (nextPosition >= endOffset) {
      return false;
    }

    ConsumerRecords<byte[], byte[]> consumerRecords = null;
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      consumerRecords = kafkaConsumer.poll(kafkaPollTimeOut);
    } catch (KafkaException ke) {
      logger.error(ke.getMessage(), ke);
      throw UserException.dataReadError(ke).message(ke.getMessage()).build(logger);
    }
    stopwatch.stop();

    if (consumerRecords.isEmpty()) {
      String errorMsg = new StringBuilder().append("Failed to fetch messages within ").append(kafkaPollTimeOut)
          .append(" milliseconds. Consider increasing the value of the property : ")
          .append(ExecConstants.KAFKA_POLL_TIMEOUT).toString();
      throw UserException.dataReadError().message(errorMsg).build(logger);
    }

    long lastFetchTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    logger.debug("Total number of messages fetched : {}", consumerRecords.count());
    logger.debug("Time taken to fetch : {} milliseconds", lastFetchTime);
    totalFetchTime += lastFetchTime;

    recordIter = consumerRecords.iterator();
    return recordIter.hasNext();
  }

  public long getTotalFetchTime() {
    return this.totalFetchTime;
  }

  @Override
  public ConsumerRecord<byte[], byte[]> next() {
    return recordIter.next();
  }
}
