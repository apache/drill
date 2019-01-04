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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.kafka.decoders.MessageReader;
import org.apache.drill.exec.store.kafka.decoders.MessageReaderFactory;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;

public class KafkaRecordReader extends AbstractRecordReader {
  private static final Logger logger = LoggerFactory.getLogger(KafkaRecordReader.class);
  public static final long DEFAULT_MESSAGES_PER_BATCH = 4000;

  private VectorContainerWriter writer;
  private MessageReader messageReader;

  private final boolean unionEnabled;
  private final KafkaStoragePlugin plugin;
  private final KafkaPartitionScanSpec subScanSpec;
  private final long kafkaPollTimeOut;

  private long currentOffset;
  private MessageIterator msgItr;

  private final boolean enableAllTextMode;
  private final boolean readNumbersAsDouble;
  private final String kafkaMsgReader;
  private int currentMessageCount;

  public KafkaRecordReader(KafkaPartitionScanSpec subScanSpec, List<SchemaPath> projectedColumns,
      FragmentContext context, KafkaStoragePlugin plugin) {
    setColumns(projectedColumns);
    final OptionManager optionManager = context.getOptions();
    this.enableAllTextMode = optionManager.getBoolean(ExecConstants.KAFKA_ALL_TEXT_MODE);
    this.readNumbersAsDouble = optionManager.getBoolean(ExecConstants.KAFKA_READER_READ_NUMBERS_AS_DOUBLE);
    this.unionEnabled = optionManager.getBoolean(ExecConstants.ENABLE_UNION_TYPE_KEY);
    this.kafkaMsgReader = optionManager.getString(ExecConstants.KAFKA_RECORD_READER);
    this.kafkaPollTimeOut = optionManager.getLong(ExecConstants.KAFKA_POLL_TIMEOUT);
    this.plugin = plugin;
    this.subScanSpec = subScanSpec;
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      for (SchemaPath column : projectedColumns) {
        transformed.add(column);
      }
    } else {
      transformed.add(SchemaPath.STAR_COLUMN);
    }
    return transformed;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.writer = new VectorContainerWriter(output, unionEnabled);
    messageReader = MessageReaderFactory.getMessageReader(kafkaMsgReader);
    messageReader.init(context.getManagedBuffer(), Lists.newArrayList(getColumns()), this.writer,
        this.enableAllTextMode, this.readNumbersAsDouble);
    msgItr = new MessageIterator(messageReader.getConsumer(plugin), subScanSpec, kafkaPollTimeOut);
  }

  /**
   * KafkaConsumer.poll will fetch 500 messages per poll call. So hasNext will
   * take care of polling multiple times for this given batch next invocation
   */
  @Override
  public int next() {
    writer.allocate();
    writer.reset();
    Stopwatch watch = Stopwatch.createStarted();
    currentMessageCount = 0;

    try {
      while (currentOffset < subScanSpec.getEndOffset() - 1 && msgItr.hasNext()) {
        ConsumerRecord<byte[], byte[]> consumerRecord = msgItr.next();
        currentOffset = consumerRecord.offset();
        writer.setPosition(currentMessageCount);
        messageReader.readMessage(consumerRecord);
        if (++currentMessageCount >= DEFAULT_MESSAGES_PER_BATCH) {
          break;
        }
      }

      if (currentMessageCount > 0) {
        messageReader.ensureAtLeastOneField();
      }
      writer.setValueCount(currentMessageCount);
      logger.debug("Took {} ms to process {} records.", watch.elapsed(TimeUnit.MILLISECONDS), currentMessageCount);
      logger.debug("Last offset consumed for {}:{} is {}", subScanSpec.getTopicName(), subScanSpec.getPartitionId(),
          currentOffset);
      return currentMessageCount;
    } catch (Exception e) {
      String msg = "Failure while reading messages from kafka. Recordreader was at record: " + (currentMessageCount + 1);
      throw UserException.dataReadError(e).message(msg).addContext(e.getMessage()).build(logger);
    }
  }

  @Override
  public void close() throws Exception {
    logger.info("Last offset processed for {}:{} is - {}", subScanSpec.getTopicName(), subScanSpec.getPartitionId(),
        currentOffset);
    logger.info("Total time to fetch messages from {}:{} is - {} milliseconds", subScanSpec.getTopicName(),
        subScanSpec.getPartitionId(), msgItr.getTotalFetchTime());
    messageReader.close();
  }

  @Override
  public String toString() {
    return "KafkaRecordReader[messageReader=" + messageReader
        + ", kafkaPollTimeOut=" + kafkaPollTimeOut
        + ", currentOffset=" + currentOffset
        + ", enableAllTextMode=" + enableAllTextMode
        + ", readNumbersAsDouble=" + readNumbersAsDouble
        + ", kafkaMsgReader=" + kafkaMsgReader
        + ", currentMessageCount=" + currentMessageCount
        + "]";
  }
}
