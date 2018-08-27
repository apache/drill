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

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class KafkaPartitionScanSpecBuilder extends
    AbstractExprVisitor<List<KafkaPartitionScanSpec>,Void,RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaPartitionScanSpecBuilder.class);
  private final LogicalExpression le;
  private final KafkaGroupScan groupScan;
  private final KafkaConsumer<?, ?> kafkaConsumer;
  private ImmutableMap<TopicPartition, KafkaPartitionScanSpec> fullScanSpec;
  private static final long CLOSE_TIMEOUT_MS = 200;

  public KafkaPartitionScanSpecBuilder(KafkaGroupScan groupScan, LogicalExpression conditionExp) {
    this.groupScan = groupScan;
    kafkaConsumer = new KafkaConsumer<>(groupScan.getKafkaStoragePluginConfig().getKafkaConsumerProps(),
        new ByteArrayDeserializer(), new ByteArrayDeserializer());
    le = conditionExp;
  }

  public List<KafkaPartitionScanSpec> parseTree() {
    ImmutableMap.Builder<TopicPartition, KafkaPartitionScanSpec> builder = ImmutableMap.builder();
    for(KafkaPartitionScanSpec scanSpec : groupScan.getPartitionScanSpecList()) {
      builder.put(new TopicPartition(scanSpec.getTopicName(), scanSpec.getPartitionId()), scanSpec);
    }
    fullScanSpec = builder.build();
    List<KafkaPartitionScanSpec> pushdownSpec = le.accept(this, null);

    /*
    Non-existing / invalid partitions may result in empty scan spec.
    This results in a "ScanBatch" with no reader. DRILL currently requires
    at least one reader to be present in a scan batch.
     */
    if(pushdownSpec != null && pushdownSpec.isEmpty()) {
      TopicPartition firstPartition = new TopicPartition(groupScan.getKafkaScanSpec().getTopicName(), 0);
      KafkaPartitionScanSpec emptySpec =
          new KafkaPartitionScanSpec(firstPartition.topic(),firstPartition.partition(),
              fullScanSpec.get(firstPartition).getEndOffset(), fullScanSpec.get(firstPartition).getEndOffset());
      pushdownSpec.add(emptySpec);
    }
    return pushdownSpec;
  }

  @Override
  public List<KafkaPartitionScanSpec> visitUnknown(LogicalExpression e, Void value)
      throws RuntimeException {
    return null;
  }

  @Override
  public List<KafkaPartitionScanSpec> visitBooleanOperator(BooleanOperator op, Void value)
      throws RuntimeException {

    Map<TopicPartition, KafkaPartitionScanSpec> specMap = Maps.newHashMap();
    ImmutableList<LogicalExpression> args = op.args;
    if(op.getName().equals("booleanOr")) {

      for(LogicalExpression expr : args) {
        List<KafkaPartitionScanSpec> parsedSpec = expr.accept(this, null);
        //parsedSpec is null if expression cannot be pushed down
        if(parsedSpec != null) {
          for(KafkaPartitionScanSpec newSpec : parsedSpec) {
            TopicPartition tp = new TopicPartition(newSpec.getTopicName(), newSpec.getPartitionId());
            KafkaPartitionScanSpec existingSpec = specMap.get(tp);

            //If existing spec does not contain topic-partition
            if(existingSpec == null) {
              specMap.put(tp, newSpec); //Add topic-partition to spec for OR
            } else {
              existingSpec.mergeScanSpec(op.getName(), newSpec);
              specMap.put(tp, existingSpec);
            }
          }
        } else {
          return null; //At any level, all arguments of booleanOr should support pushdown, else return null
        }
      }
    } else { //booleanAnd
      specMap.putAll(fullScanSpec);
      for(LogicalExpression expr : args) {
        List<KafkaPartitionScanSpec> parsedSpec = expr.accept(this, null);

        //parsedSpec is null if expression cannot be pushed down
        if(parsedSpec != null) {
          Set<TopicPartition> partitionsInNewSpec = Sets.newHashSet(); //Store topic-partitions returned from new spec.

          for (KafkaPartitionScanSpec newSpec : parsedSpec) {
            TopicPartition tp = new TopicPartition(newSpec.getTopicName(), newSpec.getPartitionId());
            partitionsInNewSpec.add(tp);
            KafkaPartitionScanSpec existingSpec = specMap.get(tp);

            if (existingSpec != null) {
              existingSpec.mergeScanSpec(op.getName(), newSpec);
              specMap.put(tp, existingSpec);
            }
          }

          /*
          For "booleanAnd", handle the case where condition is on `kafkaPartitionId`.
          In this case, we would not want unnecessarily scan all the topic-partitions.
          Hence we remove the unnecessary topic-partitions from the spec.
         */
          specMap.keySet().removeIf(partition -> !partitionsInNewSpec.contains(partition));
        }

      }
    }
    return Lists.newArrayList(specMap.values());
  }

  @Override
  public List<KafkaPartitionScanSpec> visitFunctionCall(FunctionCall call, Void value)
      throws RuntimeException {

    String functionName = call.getName();
    if(KafkaNodeProcessor.isPushdownFunction(functionName)) {

      KafkaNodeProcessor kafkaNodeProcessor = KafkaNodeProcessor.process(call);
      if(kafkaNodeProcessor.isSuccess()) {
        switch (kafkaNodeProcessor.getPath()) {
          case "kafkaMsgTimestamp":
            return createScanSpecForTimestamp(kafkaNodeProcessor.getFunctionName(),
                kafkaNodeProcessor.getValue());
          case "kafkaMsgOffset":
            return createScanSpecForOffset(kafkaNodeProcessor.getFunctionName(),
                kafkaNodeProcessor.getValue());
          case "kafkaPartitionId":
            return createScanSpecForPartition(kafkaNodeProcessor.getFunctionName(),
                kafkaNodeProcessor.getValue());
        }
      }
    }
    return null; //Return null, do not pushdown
  }


  private List<KafkaPartitionScanSpec> createScanSpecForTimestamp(String functionName,
                                                                  Long fieldValue) {
    List<KafkaPartitionScanSpec> scanSpec = Lists.newArrayList();
    Map<TopicPartition, Long> timesValMap = Maps.newHashMap();
    ImmutableSet<TopicPartition> topicPartitions = fullScanSpec.keySet();

    for(TopicPartition partitions : topicPartitions) {
      timesValMap.put(partitions, functionName.equals("greater_than") ? fieldValue+1 : fieldValue);
    }

    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp = kafkaConsumer.offsetsForTimes(timesValMap);

    for(TopicPartition tp : topicPartitions) {
      OffsetAndTimestamp value = offsetAndTimestamp.get(tp);
      //OffsetAndTimestamp is null if there is no offset greater or equal to requested timestamp
      if(value == null) {
        scanSpec.add(
            new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                fullScanSpec.get(tp).getEndOffset(), fullScanSpec.get(tp).getEndOffset()));
      } else {
        scanSpec.add(
            new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                value.offset(), fullScanSpec.get(tp).getEndOffset()));
      }
    }

    return scanSpec;
  }

  private List<KafkaPartitionScanSpec> createScanSpecForOffset(String functionName,
                                                               Long fieldValue) {
    List<KafkaPartitionScanSpec> scanSpec = Lists.newArrayList();
    ImmutableSet<TopicPartition> topicPartitions = fullScanSpec.keySet();

    /*
    We should handle the case where the specified offset does not exist in the current context,
    i.e., fieldValue < startOffset or fieldValue > endOffset in a particular topic-partition.
    Else, KafkaConsumer.poll will throw "TimeoutException".
    */

    switch (functionName) {
      case "equal":
        for(TopicPartition tp : topicPartitions) {
          if(fieldValue < fullScanSpec.get(tp).getStartOffset()) {
            //Offset does not exist
            scanSpec.add(
                new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                    fullScanSpec.get(tp).getEndOffset(), fullScanSpec.get(tp).getEndOffset()));
          } else {
            long val = Math.min(fieldValue, fullScanSpec.get(tp).getEndOffset());
            long nextVal = Math.min(val+1, fullScanSpec.get(tp).getEndOffset());
            scanSpec.add(new KafkaPartitionScanSpec(tp.topic(), tp.partition(), val, nextVal));
          }
        }
        break;
      case "greater_than_or_equal_to":
        for(TopicPartition tp : topicPartitions) {
          //Ensure scan range is between startOffset and endOffset,
          long val = bindOffsetToRange(tp, fieldValue);
          scanSpec.add(
              new KafkaPartitionScanSpec(tp.topic(), tp.partition(), val,
                  fullScanSpec.get(tp).getEndOffset()));
        }
        break;
      case "greater_than":
        for(TopicPartition tp : topicPartitions) {
          //Ensure scan range is between startOffset and endOffset,
          long val = bindOffsetToRange(tp, fieldValue+1);
          scanSpec.add(
              new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                  val, fullScanSpec.get(tp).getEndOffset()));
        }
        break;
      case "less_than_or_equal_to":
        for(TopicPartition tp : topicPartitions) {
          //Ensure scan range is between startOffset and endOffset,
          long val = bindOffsetToRange(tp, fieldValue+1);

          scanSpec.add(
              new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                  fullScanSpec.get(tp).getStartOffset(), val));
        }
        break;
      case "less_than":
        for(TopicPartition tp : topicPartitions) {
          //Ensure scan range is between startOffset and endOffset,
          long val = bindOffsetToRange(tp, fieldValue);

          scanSpec.add(
              new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                  fullScanSpec.get(tp).getStartOffset(), val));
        }
        break;
    }
    return scanSpec;
  }

  private List<KafkaPartitionScanSpec> createScanSpecForPartition(String functionName,
                                                                  Long fieldValue) {
    List<KafkaPartitionScanSpec> scanSpecList = Lists.newArrayList();
    ImmutableSet<TopicPartition> topicPartitions = fullScanSpec.keySet();

    switch (functionName) {
      case "equal":
        for(TopicPartition tp : topicPartitions) {
          if(tp.partition() == fieldValue) {
            scanSpecList.add(
                new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                    fullScanSpec.get(tp).getStartOffset(),
                    fullScanSpec.get(tp).getEndOffset()));
          }
        }
        break;
      case "not_equal":
        for(TopicPartition tp : topicPartitions) {
          if(tp.partition() != fieldValue) {
            scanSpecList.add(
                new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                    fullScanSpec.get(tp).getStartOffset(),
                    fullScanSpec.get(tp).getEndOffset()));
          }
        }
        break;
      case "greater_than_or_equal_to":
        for(TopicPartition tp : topicPartitions) {
          if(tp.partition() >= fieldValue) {
            scanSpecList.add(
                new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                    fullScanSpec.get(tp).getStartOffset(),
                    fullScanSpec.get(tp).getEndOffset()));
          }
        }
        break;
      case "greater_than":
        for(TopicPartition tp : topicPartitions) {
          if(tp.partition() > fieldValue) {
            scanSpecList.add(
                new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                    fullScanSpec.get(tp).getStartOffset(),
                    fullScanSpec.get(tp).getEndOffset()));
          }
        }
        break;
      case "less_than_or_equal_to":
        for(TopicPartition tp : topicPartitions) {
          if(tp.partition() <= fieldValue) {
            scanSpecList.add(
                new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                    fullScanSpec.get(tp).getStartOffset(),
                    fullScanSpec.get(tp).getEndOffset()));
          }
        }
        break;
      case "less_than":
        for(TopicPartition tp : topicPartitions) {
          if(tp.partition() < fieldValue) {
            scanSpecList.add(
                new KafkaPartitionScanSpec(tp.topic(), tp.partition(),
                    fullScanSpec.get(tp).getStartOffset(),
                    fullScanSpec.get(tp).getEndOffset()));
          }
        }
        break;
    }
    return scanSpecList;
  }

  void close() {
    kafkaConsumer.close(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  private long bindOffsetToRange(TopicPartition tp, long offset) {
    return Math.max(fullScanSpec.get(tp).getStartOffset(), Math.min(offset, fullScanSpec.get(tp).getEndOffset()));
  }
}
