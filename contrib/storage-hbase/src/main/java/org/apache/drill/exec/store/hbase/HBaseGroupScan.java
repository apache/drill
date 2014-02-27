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
package org.apache.drill.exec.store.hbase;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;


@JsonTypeName("hbase-scan")
public class HBaseGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseGroupScan.class);

  private ArrayListMultimap<Integer, HBaseSubScan.HBaseSubScanReadEntry> mappings;
  private Stopwatch watch = new Stopwatch();

  @JsonProperty("storage")
  public HBaseStoragePluginConfig getStorageConfig() {
    return this.storagePluginConfig;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  private String tableName;
  private HBaseStoragePlugin storagePlugin;
  private HBaseStoragePluginConfig storagePluginConfig;
  private List<EndpointAffinity> endpointAffinities;
  private List<SchemaPath> columns;

  private NavigableMap<HRegionInfo,ServerName> regionsMap;

  @JsonCreator
  public HBaseGroupScan(@JsonProperty("entries") List<HTableReadEntry> entries,
                          @JsonProperty("storage") HBaseStoragePluginConfig storageEngineConfig,
                          @JsonProperty("columns") List<SchemaPath> columns,
                          @JacksonInject StoragePluginRegistry engineRegistry
                           )throws IOException, ExecutionSetupException {
    Preconditions.checkArgument(entries.size() == 1);
    engineRegistry.init(DrillConfig.create());
    this.storagePlugin = (HBaseStoragePlugin) engineRegistry.getEngine(storageEngineConfig);
    this.storagePluginConfig = storageEngineConfig;
    this.tableName = entries.get(0).getTableName();
    this.columns = columns;
    getRegionInfos();
  }

  public HBaseGroupScan(String tableName, HBaseStoragePlugin storageEngine, List<SchemaPath> columns) throws IOException {
    this.storagePlugin = storageEngine;
    this.storagePluginConfig = storageEngine.getConfig();
    this.tableName = tableName;
    this.columns = columns;
    getRegionInfos();
  }

  protected void getRegionInfos() throws IOException {
    logger.debug("Getting region locations");
    HTable table = new HTable(storagePluginConfig.conf, tableName);
    regionsMap = table.getRegionLocations();
    regionsMap.values().iterator().next().getHostname();
    table.close();
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    watch.reset();
    watch.start();
    Map<String, DrillbitEndpoint> endpointMap = new HashMap();
    for (DrillbitEndpoint ep : storagePlugin.getContext().getBits()) {
      endpointMap.put(ep.getAddress(), ep);
    }

    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap();

    for (ServerName sn : regionsMap.values()) {
      String host = sn.getHostname();
      DrillbitEndpoint ep = endpointMap.get(host);
      EndpointAffinity affinity = affinityMap.get(ep);
      if (affinity == null) {
        affinityMap.put(ep, new EndpointAffinity(ep, 1));
      } else {
        affinity.addAffinity(1);
      }
    }
    this.endpointAffinities = Lists.newArrayList(affinityMap.values());
    logger.debug("Took {} ms to get operator affinity", watch.elapsed(TimeUnit.MILLISECONDS));
    return this.endpointAffinities;
  }

  /**
   *
   * @param incomingEndpoints
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    watch.reset();
    watch.start();
    Preconditions.checkArgument(incomingEndpoints.size() <= regionsMap.size(),
        String.format("Incoming endpoints %d is greater than number of row groups %d", incomingEndpoints.size(), regionsMap.size()));
    mappings = ArrayListMultimap.create();
    ArrayListMultimap<String, Integer> incomingEndpointMap = ArrayListMultimap.create();
    for (int i = 0; i < incomingEndpoints.size(); i++) {
      incomingEndpointMap.put(incomingEndpoints.get(i).getAddress(), i);
    }
    Map<String, Iterator<Integer>> mapIterator = new HashMap();
    for (String s : incomingEndpointMap.keySet()) {
      Iterator<Integer> ints = Iterators.cycle(incomingEndpointMap.get(s));
      mapIterator.put(s, ints);
    }
    for (HRegionInfo regionInfo : regionsMap.keySet()) {
      logger.debug("creating read entry. start key: {} end key: {}", Bytes.toStringBinary(regionInfo.getStartKey()), Bytes.toStringBinary(regionInfo.getEndKey()));
      HBaseSubScan.HBaseSubScanReadEntry p = new HBaseSubScan.HBaseSubScanReadEntry(
          tableName, Bytes.toStringBinary(regionInfo.getStartKey()), Bytes.toStringBinary(regionInfo.getEndKey()));
      String host = regionsMap.get(regionInfo).getHostname();
      mappings.put(mapIterator.get(host).next(), p);
    }
  }

  @Override
  public HBaseSubScan getSpecificScan(int minorFragmentId) {
    return new HBaseSubScan(storagePlugin, storagePluginConfig, mappings.get(minorFragmentId), columns);
  }


  @Override
  public int getMaxParallelizationWidth() {
    return regionsMap.size();
  }

  @Override
  public OperatorCost getCost() {
    //TODO Figure out how to properly calculate cost
    return new OperatorCost(1,1,1,1);
  }

  @Override
  public Size getSize() {
    // TODO - this is wrong, need to populate correctly
    return new Size(10,10);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    //TODO return copy of self
    return this;
  }

}
