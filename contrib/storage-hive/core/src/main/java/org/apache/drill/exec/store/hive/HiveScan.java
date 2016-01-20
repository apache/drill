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
package org.apache.drill.exec.store.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.hive.HiveMetadataProvider.HiveStats;
import org.apache.drill.exec.store.hive.HiveMetadataProvider.InputSplitWrapper;
import org.apache.drill.exec.store.hive.HiveTable.HivePartition;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.InputSplit;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

@JsonTypeName("hive-scan")
public class HiveScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveScan.class);

  private static int HIVE_SERDE_SCAN_OVERHEAD_FACTOR_PER_COLUMN = 20;

  @JsonProperty("hive-table")
  public HiveReadEntry hiveReadEntry;

  @JsonIgnore
  public HiveStoragePlugin storagePlugin;

  @JsonProperty("columns")
  public List<SchemaPath> columns;

  @JsonIgnore
  protected final HiveMetadataProvider metadataProvider;

  @JsonIgnore
  private List<List<InputSplitWrapper>> mappings;

  @JsonIgnore
  protected List<InputSplitWrapper> inputSplits;

  @JsonCreator
  public HiveScan(@JsonProperty("userName") final String userName,
                  @JsonProperty("hive-table") final HiveReadEntry hiveReadEntry,
                  @JsonProperty("storage-plugin") final String storagePluginName,
                  @JsonProperty("columns") final List<SchemaPath> columns,
                  @JacksonInject final StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    this(userName, hiveReadEntry, (HiveStoragePlugin) pluginRegistry.getPlugin(storagePluginName), columns, null);
  }

  public HiveScan(final String userName, final HiveReadEntry hiveReadEntry, final HiveStoragePlugin storagePlugin,
      final List<SchemaPath> columns, final HiveMetadataProvider metadataProvider) throws ExecutionSetupException {
    super(userName);
    this.hiveReadEntry = hiveReadEntry;
    this.columns = columns;
    this.storagePlugin = storagePlugin;
    if (metadataProvider == null) {
      this.metadataProvider = new HiveMetadataProvider(userName, hiveReadEntry, storagePlugin.getHiveConf());
    } else {
      this.metadataProvider = metadataProvider;
    }
  }

  public HiveScan(final HiveScan that) {
    super(that);
    this.columns = that.columns;
    this.hiveReadEntry = that.hiveReadEntry;
    this.storagePlugin = that.storagePlugin;
    this.metadataProvider = that.metadataProvider;
  }

  public HiveScan clone(final HiveReadEntry hiveReadEntry) throws ExecutionSetupException {
    return new HiveScan(getUserName(), hiveReadEntry, storagePlugin, columns, metadataProvider);
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  protected List<InputSplitWrapper> getInputSplits() {
    if (inputSplits == null) {
      inputSplits = metadataProvider.getInputSplits(hiveReadEntry);
    }

    return inputSplits;
  }

  @Override
  public void applyAssignments(final List<CoordinationProtos.DrillbitEndpoint> endpoints) {
    mappings = Lists.newArrayList();
    for (int i = 0; i < endpoints.size(); i++) {
      mappings.add(new ArrayList<InputSplitWrapper>());
    }
    final int count = endpoints.size();
    final List<InputSplitWrapper> inputSplits = getInputSplits();
    for (int i = 0; i < inputSplits.size(); i++) {
      mappings.get(i % count).add(inputSplits.get(i));
    }
  }

  public static String serializeInputSplit(final InputSplit split) throws IOException {
    final ByteArrayDataOutput byteArrayOutputStream =  ByteStreams.newDataOutput();
    split.write(byteArrayOutputStream);
    final String encoded = Base64.encodeBase64String(byteArrayOutputStream.toByteArray());
    logger.debug("Encoded split string for split {} : {}", split, encoded);
    return encoded;
  }

  @Override
  public SubScan getSpecificScan(final int minorFragmentId) throws ExecutionSetupException {
    try {
      final List<InputSplitWrapper> splits = mappings.get(minorFragmentId);
      List<HivePartition> parts = Lists.newArrayList();
      final List<String> encodedInputSplits = Lists.newArrayList();
      final List<String> splitTypes = Lists.newArrayList();
      for (final InputSplitWrapper split : splits) {
        if (split.getPartition() != null) {
          parts.add(new HivePartition(split.getPartition()));
        }

        encodedInputSplits.add(serializeInputSplit(split.getSplit()));
        splitTypes.add(split.getSplit().getClass().getName());
      }
      if (parts.size() <= 0) {
        parts = null;
      }

      final HiveReadEntry subEntry = new HiveReadEntry(hiveReadEntry.table, parts);
      return new HiveSubScan(getUserName(), encodedInputSplits, subEntry, splitTypes, columns, storagePlugin);
    } catch (IOException | ReflectiveOperationException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int getMaxParallelizationWidth() {
    return getInputSplits().size();
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    final Map<String, DrillbitEndpoint> endpointMap = new HashMap<>();
    for (final DrillbitEndpoint endpoint : storagePlugin.getContext().getBits()) {
      endpointMap.put(endpoint.getAddress(), endpoint);
      logger.debug("endpoing address: {}", endpoint.getAddress());
    }
    final Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<>();
    try {
      long totalSize = 0;
      final List<InputSplitWrapper> inputSplits = getInputSplits();
      for (final InputSplitWrapper split : inputSplits) {
        totalSize += Math.max(1, split.getSplit().getLength());
      }
      for (final InputSplitWrapper split : inputSplits) {
        final float affinity = ((float) Math.max(1, split.getSplit().getLength())) / totalSize;
        for (final String loc : split.getSplit().getLocations()) {
          logger.debug("split location: {}", loc);
          final DrillbitEndpoint endpoint = endpointMap.get(loc);
          if (endpoint != null) {
            if (affinityMap.containsKey(endpoint)) {
              affinityMap.get(endpoint).addAffinity(affinity);
            } else {
              affinityMap.put(endpoint, new EndpointAffinity(endpoint, affinity));
            }
          }
        }
      }
    } catch (final IOException e) {
      throw new DrillRuntimeException(e);
    }
    for (final DrillbitEndpoint ep : affinityMap.keySet()) {
      Preconditions.checkNotNull(ep);
    }
    for (final EndpointAffinity a : affinityMap.values()) {
      Preconditions.checkNotNull(a.getEndpoint());
    }
    return Lists.newArrayList(affinityMap.values());
  }

  @Override
  public ScanStats getScanStats() {
    try {
      final HiveStats stats = metadataProvider.getStats(hiveReadEntry);

      logger.debug("HiveStats: {}", stats.toString());

      // Hive's native reader is neither memory efficient nor fast. Increase the CPU cost
      // by a factor to let the planner choose HiveDrillNativeScan over HiveScan with SerDes.
      float cpuCost = 1 * getSerDeOverheadFactor();
      return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, stats.getNumRows(), cpuCost, stats.getSizeInBytes());
    } catch (final IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

  protected int getSerDeOverheadFactor() {
    final int projectedColumnCount;
    if (AbstractRecordReader.isStarQuery(columns)) {
      Table hiveTable = hiveReadEntry.getTable();
      projectedColumnCount = hiveTable.getSd().getColsSize() + hiveTable.getPartitionKeysSize();
    } else {
      // In cost estimation, # of project columns should be >= 1, even for skipAll query.
      projectedColumnCount = Math.max(columns.size(), 1);
    }

    return projectedColumnCount * HIVE_SERDE_SCAN_OVERHEAD_FACTOR_PER_COLUMN;
  }

  @Override
  public PhysicalOperator getNewWithChildren(final List<PhysicalOperator> children) throws ExecutionSetupException {
    return new HiveScan(this);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    List<HivePartition> partitions = hiveReadEntry.getHivePartitionWrappers();
    int numPartitions = partitions == null ? 0 : partitions.size();
    return "HiveScan [table=" + hiveReadEntry.getHiveTableWrapper()
        + ", columns=" + columns
        + ", numPartitions=" + numPartitions
        + ", partitions= " + partitions
        + ", inputDirectories=" + metadataProvider.getInputDirectories(hiveReadEntry)
        + "]";
  }

  @Override
  public GroupScan clone(final List<SchemaPath> columns) {
    final HiveScan newScan = new HiveScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public boolean canPushdownProjects(final List<SchemaPath> columns) {
    return true;
  }

  // Return true if the current table is partitioned false otherwise
  public boolean supportsPartitionFilterPushdown() {
    final List<FieldSchema> partitionKeys = hiveReadEntry.getTable().getPartitionKeys();
    if (partitionKeys == null || partitionKeys.size() == 0) {
      return false;
    }
    return true;
  }

  @JsonIgnore
  public HiveConf getHiveConf() {
    return storagePlugin.getHiveConf();
  }

  @JsonIgnore
  public boolean isNativeReader() {
    return false;
  }
}
