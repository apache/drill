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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.kafka.KafkaSubScan.KafkaSubScanSpec;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@JsonTypeName("kafka-scan")
public class KafkaGroupScan extends AbstractGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(KafkaGroupScan.class);

  // Assuming default average topic message size as 1KB, which will be used to
  // compute the stats and work assignments
  private static final long MSG_SIZE = 1024;

  private final KafkaStoragePlugin kafkaStoragePlugin;
  private final KafkaStoragePluginConfig kafkaStoragePluginConfig;
  private List<SchemaPath> columns;
  private final KafkaScanSpec kafkaScanSpec;

  private List<PartitionScanWork> partitionWorkList;
  private ListMultimap<Integer, PartitionScanWork> assignments;
  private List<EndpointAffinity> affinities;

  @JsonCreator
  public KafkaGroupScan(@JsonProperty("userName") String userName,
      @JsonProperty("kafkaStoragePluginConfig") KafkaStoragePluginConfig kafkaStoragePluginConfig,
      @JsonProperty("columns") List<SchemaPath> columns, @JsonProperty("scanSpec") KafkaScanSpec scanSpec,
      @JacksonInject StoragePluginRegistry pluginRegistry) {
    this(userName, kafkaStoragePluginConfig, columns, scanSpec, (KafkaStoragePlugin) pluginRegistry);
  }

  public KafkaGroupScan(KafkaStoragePlugin kafkaStoragePlugin, KafkaScanSpec kafkaScanSpec, List<SchemaPath> columns) {
    super(StringUtils.EMPTY);
    this.kafkaStoragePlugin = kafkaStoragePlugin;
    this.kafkaStoragePluginConfig = (KafkaStoragePluginConfig) kafkaStoragePlugin.getConfig();
    this.columns = columns;
    this.kafkaScanSpec = kafkaScanSpec;
    init();
  }

  public KafkaGroupScan(String userName, KafkaStoragePluginConfig kafkaStoragePluginConfig, List<SchemaPath> columns,
      KafkaScanSpec kafkaScanSpec, KafkaStoragePlugin pluginRegistry) {
    super(userName);
    this.kafkaStoragePluginConfig = kafkaStoragePluginConfig;
    this.columns = columns;
    this.kafkaScanSpec = kafkaScanSpec;
    this.kafkaStoragePlugin = pluginRegistry;
    init();
  }

  public KafkaGroupScan(KafkaGroupScan that) {
    super(that);
    this.kafkaStoragePluginConfig = that.kafkaStoragePluginConfig;
    this.columns = that.columns;
    this.kafkaScanSpec = that.kafkaScanSpec;
    this.kafkaStoragePlugin = that.kafkaStoragePlugin;
    this.partitionWorkList = that.partitionWorkList;
    this.assignments = that.assignments;
  }

  private static class PartitionScanWork implements CompleteWork {

    private final EndpointByteMapImpl byteMap = new EndpointByteMapImpl();

    private final TopicPartition topicPartition;
    private final long beginOffset;
    private final long latestOffset;

    public PartitionScanWork(TopicPartition topicPartition, long beginOffset, long latestOffset) {
      this.topicPartition = topicPartition;
      this.beginOffset = beginOffset;
      this.latestOffset = latestOffset;
    }

    public TopicPartition getTopicPartition() {
      return topicPartition;
    }

    public long getBeginOffset() {
      return beginOffset;
    }

    public long getLatestOffset() {
      return latestOffset;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return Long.compare(getTotalBytes(), o.getTotalBytes());
    }

    @Override
    public long getTotalBytes() {
      return (latestOffset - beginOffset) * MSG_SIZE;
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

  }

  /**
   * Computes work per topic partition, based on start and end offset of each
   * corresponding topicPartition
   */
  private void init() {
    partitionWorkList = Lists.newArrayList();
    Collection<DrillbitEndpoint> endpoints = kafkaStoragePlugin.getContext().getBits();
    Map<String, DrillbitEndpoint> endpointMap = Maps.newHashMap();
    for (DrillbitEndpoint endpoint : endpoints) {
      endpointMap.put(endpoint.getAddress(), endpoint);
    }

    Map<TopicPartition, Long> startOffsetsMap = Maps.newHashMap();
    Map<TopicPartition, Long> endOffsetsMap = Maps.newHashMap();
    List<PartitionInfo> topicPartitions = null;
    String topicName = kafkaScanSpec.getTopicName();

    try (KafkaConsumer<?, ?> kafkaConsumer = new KafkaConsumer<>(kafkaStoragePlugin.getConfig().getKafkaConsumerProps(),
        new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      if (!kafkaConsumer.listTopics().keySet().contains(topicName)) {
        throw UserException.dataReadError()
            .message("Table '%s' does not exist", topicName)
            .build(logger);
      }

      kafkaConsumer.subscribe(Arrays.asList(topicName));
      // based on KafkaConsumer JavaDoc, seekToBeginning/seekToEnd functions
      // evaluates lazily, seeking to the first/last offset in all partitions only
      // when poll(long) or
      // position(TopicPartition) are called
      kafkaConsumer.poll(0);
      Set<TopicPartition> assignments = kafkaConsumer.assignment();
      topicPartitions = kafkaConsumer.partitionsFor(topicName);

      // fetch start offsets for each topicPartition
      kafkaConsumer.seekToBeginning(assignments);
      for (TopicPartition topicPartition : assignments) {
        startOffsetsMap.put(topicPartition, kafkaConsumer.position(topicPartition));
      }

      // fetch end offsets for each topicPartition
      kafkaConsumer.seekToEnd(assignments);
      for (TopicPartition topicPartition : assignments) {
        endOffsetsMap.put(topicPartition, kafkaConsumer.position(topicPartition));
      }
    } catch (Exception e) {
      throw UserException.dataReadError(e).message("Failed to fetch start/end offsets of the topic  %s", topicName)
          .addContext(e.getMessage()).build(logger);
    }

    // computes work for each end point
    for (PartitionInfo partitionInfo : topicPartitions) {
      TopicPartition topicPartition = new TopicPartition(topicName, partitionInfo.partition());
      long lastCommittedOffset = startOffsetsMap.get(topicPartition);
      long latestOffset = endOffsetsMap.get(topicPartition);
      logger.debug("Latest offset of {} is {}", topicPartition, latestOffset);
      logger.debug("Last committed offset of {} is {}", topicPartition, lastCommittedOffset);
      PartitionScanWork work = new PartitionScanWork(topicPartition, lastCommittedOffset, latestOffset);
      Node[] inSyncReplicas = partitionInfo.inSyncReplicas();
      for (Node isr : inSyncReplicas) {
        String host = isr.host();
        DrillbitEndpoint ep = endpointMap.get(host);
        if (ep != null) {
          work.getByteMap().add(ep, work.getTotalBytes());
        }
      }
      partitionWorkList.add(work);
    }
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    assignments = AssignmentCreator.getMappings(incomingEndpoints, partitionWorkList);
  }

  @Override
  public KafkaSubScan getSpecificScan(int minorFragmentId) {
    List<PartitionScanWork> workList = assignments.get(minorFragmentId);
    List<KafkaSubScanSpec> scanSpecList = Lists.newArrayList();

    for (PartitionScanWork work : workList) {
      scanSpecList.add(new KafkaSubScanSpec(work.getTopicPartition().topic(), work.getTopicPartition().partition(),
          work.getBeginOffset(), work.getLatestOffset()));
    }

    return new KafkaSubScan(getUserName(), kafkaStoragePlugin, kafkaStoragePluginConfig, columns, scanSpecList);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return partitionWorkList.size();
  }

  @Override
  public ScanStats getScanStats() {
    long messageCount = 0;
    for (PartitionScanWork work : partitionWorkList) {
      messageCount += (work.getLatestOffset() - work.getBeginOffset());
    }
    return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, messageCount, 1, messageCount * MSG_SIZE);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new KafkaGroupScan(this);
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (affinities == null) {
      affinities = AffinityCreator.getAffinityMap(partitionWorkList);
    }
    return affinities;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    KafkaGroupScan clone = new KafkaGroupScan(this);
    clone.columns = columns;
    return clone;
  }

  @JsonProperty("kafkaStoragePluginConfig")
  public KafkaStoragePluginConfig getStorageConfig() {
    return this.kafkaStoragePluginConfig;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("kafkaScanSpec")
  public KafkaScanSpec getScanSpec() {
    return kafkaScanSpec;
  }

  @JsonIgnore
  public KafkaStoragePlugin getStoragePlugin() {
    return kafkaStoragePlugin;
  }

  @Override
  public String toString() {
    return String.format("KafkaGroupScan [KafkaScanSpec=%s, columns=%s]", kafkaScanSpec, columns);
  }

}
