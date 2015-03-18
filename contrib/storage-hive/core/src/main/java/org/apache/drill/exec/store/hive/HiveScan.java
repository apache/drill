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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.hive.HiveTable.HivePartition;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

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

  @JsonProperty("hive-table")
  public HiveReadEntry hiveReadEntry;
  @JsonIgnore
  private List<InputSplit> inputSplits = Lists.newArrayList();
  @JsonIgnore
  public HiveStoragePlugin storagePlugin;
  @JsonProperty("storage-plugin")
  public String storagePluginName;

  @JsonIgnore
  private final Collection<DrillbitEndpoint> endpoints;

  @JsonProperty("columns")
  public List<SchemaPath> columns;

  @JsonIgnore
  List<List<InputSplit>> mappings;

  @JsonIgnore
  Map<InputSplit, Partition> partitionMap = new HashMap();

  /*
   * total number of rows (obtained from metadata store)
   */
  @JsonIgnore
  private long rowCount = 0;

  @JsonCreator
  public HiveScan(@JsonProperty("userName") final String userName,
                  @JsonProperty("hive-table") final HiveReadEntry hiveReadEntry,
                  @JsonProperty("storage-plugin") final String storagePluginName,
                  @JsonProperty("columns") final List<SchemaPath> columns,
                  @JacksonInject final StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    super(userName);
    this.hiveReadEntry = hiveReadEntry;
    this.storagePluginName = storagePluginName;
    this.storagePlugin = (HiveStoragePlugin) pluginRegistry.getPlugin(storagePluginName);
    this.columns = columns;
    getSplits();
    endpoints = storagePlugin.getContext().getBits();
  }

  public HiveScan(final String userName, final HiveReadEntry hiveReadEntry, final HiveStoragePlugin storagePlugin, final List<SchemaPath> columns) throws ExecutionSetupException {
    super(userName);
    this.hiveReadEntry = hiveReadEntry;
    this.columns = columns;
    this.storagePlugin = storagePlugin;
    getSplits();
    endpoints = storagePlugin.getContext().getBits();
    this.storagePluginName = storagePlugin.getName();
  }

  private HiveScan(final HiveScan that) {
    super(that);
    this.columns = that.columns;
    this.endpoints = that.endpoints;
    this.hiveReadEntry = that.hiveReadEntry;
    this.inputSplits = that.inputSplits;
    this.mappings = that.mappings;
    this.partitionMap = that.partitionMap;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginName = that.storagePluginName;
    this.rowCount = that.rowCount;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  private void getSplits() throws ExecutionSetupException {
    try {
      final List<Partition> partitions = hiveReadEntry.getPartitions();
      final Table table = hiveReadEntry.getTable();
      if (partitions == null || partitions.size() == 0) {
        final Properties properties = MetaStoreUtils.getTableMetadata(table);
        splitInput(properties, table.getSd(), null);
      } else {
        for (final Partition partition : partitions) {
          final Properties properties = MetaStoreUtils.getPartitionMetadata(partition, table);
          splitInput(properties, partition.getSd(), partition);
        }
      }
    } catch (ReflectiveOperationException | IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  /* Split the input given in StorageDescriptor */
  private void splitInput(final Properties properties, final StorageDescriptor sd, final Partition partition)
      throws ReflectiveOperationException, IOException {
    final JobConf job = new JobConf();
    for (final Object obj : properties.keySet()) {
      job.set((String) obj, (String) properties.get(obj));
    }
    for (final Map.Entry<String, String> entry : hiveReadEntry.hiveConfigOverride.entrySet()) {
      job.set(entry.getKey(), entry.getValue());
    }
    InputFormat<?, ?> format = (InputFormat<?, ?>)
        Class.forName(sd.getInputFormat()).getConstructor().newInstance();
    job.setInputFormat(format.getClass());
    final Path path = new Path(sd.getLocation());
    final FileSystem fs = path.getFileSystem(job);

    // Use new JobConf that has FS configuration
    final JobConf jobWithFsConf = new JobConf(fs.getConf());
    if (fs.exists(path)) {
      FileInputFormat.addInputPath(jobWithFsConf, path);
      format = jobWithFsConf.getInputFormat();
      for (final InputSplit split : format.getSplits(jobWithFsConf, 1)) {
        inputSplits.add(split);
        partitionMap.put(split, partition);
      }
    }
    final String numRowsProp = properties.getProperty("numRows");
    logger.trace("HiveScan num rows property = {}", numRowsProp);
    if (numRowsProp != null) {
      final long numRows = Long.valueOf(numRowsProp);
      // starting from hive-0.13, when no statistics are available, this property is set to -1
      // it's important to note that the value returned by hive may not be up to date
      if (numRows > 0) {
        rowCount += numRows;
      }
    }
  }

  @Override
  public void applyAssignments(final List<CoordinationProtos.DrillbitEndpoint> endpoints) {
    mappings = Lists.newArrayList();
    for (int i = 0; i < endpoints.size(); i++) {
      mappings.add(new ArrayList<InputSplit>());
    }
    final int count = endpoints.size();
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
      final List<InputSplit> splits = mappings.get(minorFragmentId);
      List<HivePartition> parts = Lists.newArrayList();
      final List<String> encodedInputSplits = Lists.newArrayList();
      final List<String> splitTypes = Lists.newArrayList();
      for (final InputSplit split : splits) {
        HivePartition partition = null;
        if (partitionMap.get(split) != null) {
          partition = new HivePartition(partitionMap.get(split));
        }
        parts.add(partition);
        encodedInputSplits.add(serializeInputSplit(split));
        splitTypes.add(split.getClass().getName());
      }
      if (parts.contains(null)) {
        parts = null;
      }

      final HiveReadEntry subEntry = new HiveReadEntry(hiveReadEntry.table, parts, hiveReadEntry.hiveConfigOverride);
      return new HiveSubScan(getUserName(), encodedInputSplits, subEntry, splitTypes, columns);
    } catch (IOException | ReflectiveOperationException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int getMaxParallelizationWidth() {
    return inputSplits.size();
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    final Map<String, DrillbitEndpoint> endpointMap = new HashMap<>();
    for (final DrillbitEndpoint endpoint : endpoints) {
      endpointMap.put(endpoint.getAddress(), endpoint);
      logger.debug("endpoing address: {}", endpoint.getAddress());
    }
    final Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<>();
    try {
      long totalSize = 0;
      for (final InputSplit split : inputSplits) {
        totalSize += Math.max(1, split.getLength());
      }
      for (final InputSplit split : inputSplits) {
        final float affinity = ((float) Math.max(1, split.getLength())) / totalSize;
        for (final String loc : split.getLocations()) {
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
      long data =0;
      for (final InputSplit split : inputSplits) {
          data += split.getLength();
      }

      long estRowCount = rowCount;
      if (estRowCount == 0) {
        // having a rowCount of 0 can mean the statistics were never computed
        estRowCount = data/1024;
      }
      logger.debug("estimated row count = {}, stats row count = {}", estRowCount, rowCount);
      return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, 1, data);
    } catch (final IOException e) {
      throw new DrillRuntimeException(e);
    }
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
    return "HiveScan [table=" + hiveReadEntry.getHiveTableWrapper()
        + ", inputSplits=" + inputSplits
        + ", columns=" + columns
        + ", partitions= " + hiveReadEntry.getHivePartitionWrappers() +"]";
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
}
