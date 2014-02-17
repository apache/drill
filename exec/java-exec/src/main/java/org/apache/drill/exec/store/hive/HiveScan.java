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

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.commons.codec.binary.Base64;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.*;

@JsonTypeName("hive-scan")
public class HiveScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveScan.class);

  @JsonProperty("hive-table")
  public HiveReadEntry hiveReadEntry;
  @JsonIgnore
  private Table table;
  @JsonIgnore
  private List<InputSplit> inputSplits = Lists.newArrayList();
  @JsonIgnore
  public HiveStoragePlugin storagePlugin;
  @JsonProperty("storage-plugin")
  public String storagePluginName;

  @JsonIgnore
  public List<Partition> partitions;
  @JsonIgnore
  private Collection<DrillbitEndpoint> endpoints;

  @JsonProperty("columns")
  public List<FieldReference> columns;

  @JsonIgnore
  List<List<InputSplit>> mappings;

  @JsonIgnore
  Map<InputSplit, Partition> partitionMap = new HashMap();

  @JsonCreator
  public HiveScan(@JsonProperty("hive-table") HiveReadEntry hiveReadEntry, @JsonProperty("storage-plugin") String storagePluginName,
                  @JsonProperty("columns") List<FieldReference> columns,
                  @JacksonInject StoragePluginRegistry engineRegistry) throws ExecutionSetupException {
    this.hiveReadEntry = hiveReadEntry;
    this.table = hiveReadEntry.getTable();
    this.storagePluginName = storagePluginName;
    this.storagePlugin = (HiveStoragePlugin) engineRegistry.getEngine(storagePluginName);
    this.columns = columns;
    this.partitions = hiveReadEntry.getPartitions();
    getSplits();
    endpoints = storagePlugin.getContext().getBits();
  }

  public HiveScan(HiveReadEntry hiveReadEntry, HiveStoragePlugin storageEngine, List<FieldReference> columns) throws ExecutionSetupException {
    this.table = hiveReadEntry.getTable();
    this.hiveReadEntry = hiveReadEntry;
    this.columns = columns;
    this.partitions = hiveReadEntry.getPartitions();
    getSplits();
    endpoints = storageEngine.getContext().getBits();
    this.storagePluginName = storageEngine.getName();
  }

  public List<FieldReference> getColumns() {
    return columns;
  }

  private void getSplits() throws ExecutionSetupException {
    try {
      if (partitions == null || partitions.size() == 0) {
        Properties properties = MetaStoreUtils.getTableMetadata(table);
        JobConf job = new JobConf();
        for (Object obj : properties.keySet()) {
          job.set((String) obj, (String) properties.get(obj));
        }
        InputFormat<?, ?> format = (InputFormat<?, ?>) Class.forName(table.getSd().getInputFormat()).getConstructor().newInstance();
        job.setInputFormat(format.getClass());
        Path path = new Path(table.getSd().getLocation());
        FileInputFormat.addInputPath(job, path);
        format = job.getInputFormat();
        for (InputSplit split : format.getSplits(job, 1)) {
          inputSplits.add(split);
        }
        for (InputSplit split : inputSplits) {
          partitionMap.put(split, null);
        }
      } else {
        for (Partition partition : partitions) {
          Properties properties = MetaStoreUtils.getPartitionMetadata(partition, table);
          JobConf job = new JobConf();
          for (Object obj : properties.keySet()) {
            job.set((String) obj, (String) properties.get(obj));
          }
          InputFormat<?, ?> format = (InputFormat<?, ?>) Class.forName(partition.getSd().getInputFormat()).getConstructor().newInstance();
          job.setInputFormat(format.getClass());
          FileInputFormat.addInputPath(job, new Path(partition.getSd().getLocation()));
          format = job.getInputFormat();
          InputSplit[] splits = format.getSplits(job,1);
          for (InputSplit split : splits) {
            inputSplits.add(split);
            partitionMap.put(split, partition);
          }
        }
      }
    } catch (ReflectiveOperationException | IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
    mappings = Lists.newArrayList();
    for (int i = 0; i < endpoints.size(); i++) {
      mappings.add(new ArrayList<InputSplit>());
    }
    int count = endpoints.size();
    for (int i = 0; i < inputSplits.size(); i++) {
      mappings.get(i % count).add(inputSplits.get(i));
    }
  }

  public static String serializeInputSplit(InputSplit split) throws IOException {
    ByteArrayDataOutput byteArrayOutputStream =  ByteStreams.newDataOutput();
    split.write(byteArrayOutputStream);
    String encoded = Base64.encodeBase64String(byteArrayOutputStream.toByteArray());
    logger.debug("Encoded split string for split {} : {}", split, encoded);
    return encoded;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    try {
      List<InputSplit> splits = mappings.get(minorFragmentId);
      List<Partition> parts = Lists.newArrayList();
      List<String> encodedInputSplits = Lists.newArrayList();
      List<String> splitTypes = Lists.newArrayList();
      for (InputSplit split : splits) {
        parts.add(partitionMap.get(split));
        encodedInputSplits.add(serializeInputSplit(split));
        splitTypes.add(split.getClass().getCanonicalName());
      }
      if (parts.contains(null)) parts = null;
      return new HiveSubScan(table, parts, encodedInputSplits, splitTypes, columns);
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
    Map<String, DrillbitEndpoint> endpointMap = new HashMap<>();
    for (DrillbitEndpoint endpoint : endpoints) {
      endpointMap.put(endpoint.getAddress(), endpoint);
      logger.debug("endpoing address: {}", endpoint.getAddress());
    }
    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<>();
    try {
      long totalSize = 0;
      for (InputSplit split : inputSplits) {
        totalSize += Math.max(1, split.getLength());
      }
      for (InputSplit split : inputSplits) {
        float affinity = ((float) Math.max(1, split.getLength())) / totalSize;
        for (String loc : split.getLocations()) {
          logger.debug("split location: {}", loc);
          DrillbitEndpoint endpoint = endpointMap.get(loc);
          if (endpoint != null) {
            if (affinityMap.containsKey(endpoint)) {
              affinityMap.get(endpoint).addAffinity(affinity);
            } else {
              affinityMap.put(endpoint, new EndpointAffinity(endpoint, affinity));
            }
          }
        }
      }
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
    for (DrillbitEndpoint ep : affinityMap.keySet()) {
      Preconditions.checkNotNull(ep);
    }
    for (EndpointAffinity a : affinityMap.values()) {
      Preconditions.checkNotNull(a.getEndpoint());
    }
    return Lists.newArrayList(affinityMap.values());
  }

  @Override
  public OperatorCost getCost() {
    return new OperatorCost(1, 2, 1, 1);
  }

  @Override
  public Size getSize() {
    // TODO - this is wrong, need to populate correctly
    return new Size(10,10);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return new HiveScan(hiveReadEntry, storagePlugin, columns);
  }
}
