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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

@JsonTypeName("hbase-scan")
public class HBaseGroupScan extends AbstractGroupScan implements DrillHBaseConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseGroupScan.class);

  private HBaseStoragePluginConfig storagePluginConfig;

  private List<SchemaPath> columns;

  private HBaseScanSpec hbaseScanSpec;

  private HBaseStoragePlugin storagePlugin;

  private Stopwatch watch = new Stopwatch();
  private ArrayListMultimap<Integer, HBaseSubScan.HBaseSubScanSpec> mappings;
  private List<EndpointAffinity> endpointAffinities;
  private NavigableMap<HRegionInfo,ServerName> regionsToScan;
  private HTableDescriptor hTableDesc;

  private boolean filterPushedDown = false;

  @JsonCreator
  public HBaseGroupScan(@JsonProperty("hbaseScanSpec") HBaseScanSpec hbaseScanSpec,
                        @JsonProperty("storage") HBaseStoragePluginConfig storagePluginConfig,
                        @JsonProperty("columns") List<SchemaPath> columns,
                        @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this ((HBaseStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), hbaseScanSpec, columns);
  }

  public HBaseGroupScan(HBaseStoragePlugin storageEngine, HBaseScanSpec scanSpec, List<SchemaPath> columns) {
    this.storagePlugin = storageEngine;
    this.storagePluginConfig = storageEngine.getConfig();
    this.hbaseScanSpec = scanSpec;
    this.columns = columns;
    init();
  }

  /**
   * Private constructor, used for cloning.
   * @param that The
   */
  private HBaseGroupScan(HBaseGroupScan that) {
    this.columns = that.columns;
    this.endpointAffinities = that.endpointAffinities;
    this.hbaseScanSpec = that.hbaseScanSpec;
    this.mappings = that.mappings;
    this.regionsToScan = that.regionsToScan;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.hTableDesc = that.hTableDesc;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    HBaseGroupScan newScan = new HBaseGroupScan(this);
    newScan.columns = columns;
    newScan.verifyColumns();
    return newScan;
  }

  private void init() {
    logger.debug("Getting region locations");
    try {
      HTable table = new HTable(storagePluginConfig.getHBaseConf(), hbaseScanSpec.getTableName());
      this.hTableDesc = table.getTableDescriptor();
      NavigableMap<HRegionInfo, ServerName> regionsMap = table.getRegionLocations();
      table.close();

      boolean foundStartRegion = false;
      regionsToScan = new TreeMap<HRegionInfo, ServerName>();
      for (Entry<HRegionInfo, ServerName> mapEntry : regionsMap.entrySet()) {
        HRegionInfo regionInfo = mapEntry.getKey();
        if (!foundStartRegion && hbaseScanSpec.getStartRow() != null && hbaseScanSpec.getStartRow().length != 0 && !regionInfo.containsRow(hbaseScanSpec.getStartRow())) {
          continue;
        }
        foundStartRegion = true;
        regionsToScan.put(regionInfo, mapEntry.getValue());
        if (hbaseScanSpec.getStopRow() != null && hbaseScanSpec.getStopRow().length != 0 && regionInfo.containsRow(hbaseScanSpec.getStopRow())) {
          break;
        }
      }
    } catch (IOException e) {
      throw new DrillRuntimeException("Error getting region info for table: " + hbaseScanSpec.getTableName(), e);
    }
    verifyColumns();
  }

  private void verifyColumns() {
    if (columns != null) {
      for (SchemaPath column : columns) {
        if (!(column.equals(ROW_KEY_PATH) || hTableDesc.hasFamily(HBaseUtils.getBytes(column.getRootSegment().getPath())))) {
          DrillRuntimeException.format("The column family '%s' does not exist in HBase table: %s .",
              column.getRootSegment().getPath(), hTableDesc.getNameAsString());
        }
      }
    }
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    watch.reset();
    watch.start();
    Map<String, DrillbitEndpoint> endpointMap = new HashMap<String, DrillbitEndpoint>();
    for (DrillbitEndpoint ep : storagePlugin.getContext().getBits()) {
      endpointMap.put(ep.getAddress(), ep);
    }

    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<DrillbitEndpoint, EndpointAffinity>();
    for (ServerName sn : regionsToScan.values()) {
      String host = sn.getHostname();
      DrillbitEndpoint ep = endpointMap.get(host);
      if (ep != null) {
        EndpointAffinity affinity = affinityMap.get(ep);
        if (affinity == null) {
          affinityMap.put(ep, new EndpointAffinity(ep, 1));
        } else {
          affinity.addAffinity(1);
        }
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
    Preconditions.checkArgument(incomingEndpoints.size() <= regionsToScan.size(),
        String.format("Incoming endpoints %d is greater than number of row groups %d", incomingEndpoints.size(), regionsToScan.size()));

    mappings = ArrayListMultimap.create();
    ArrayListMultimap<String, Integer> incomingEndpointMap = ArrayListMultimap.create();
    for (int i = 0; i < incomingEndpoints.size(); i++) {
      incomingEndpointMap.put(incomingEndpoints.get(i).getAddress(), i);
    }
    Map<String, Iterator<Integer>> mapIterator = new HashMap<String, Iterator<Integer>>();
    for (String s : incomingEndpointMap.keySet()) {
      Iterator<Integer> ints = Iterators.cycle(incomingEndpointMap.get(s));
      mapIterator.put(s, ints);
    }
    Iterator<Integer> nullIterator = Iterators.cycle(incomingEndpointMap.values());
    for (HRegionInfo regionInfo : regionsToScan.keySet()) {
      logger.debug("creating read entry. start key: {} end key: {}", Bytes.toStringBinary(regionInfo.getStartKey()), Bytes.toStringBinary(regionInfo.getEndKey()));
      HBaseSubScan.HBaseSubScanSpec p = new HBaseSubScan.HBaseSubScanSpec()
          .setTableName(hbaseScanSpec.getTableName())
          .setStartRow((hbaseScanSpec.getStartRow() != null && regionInfo.containsRow(hbaseScanSpec.getStartRow())) ? hbaseScanSpec.getStartRow() : regionInfo.getStartKey())
          .setStopRow((hbaseScanSpec.getStopRow() != null && regionInfo.containsRow(hbaseScanSpec.getStopRow())) ? hbaseScanSpec.getStopRow() : regionInfo.getEndKey())
          .setSerializedFilter(hbaseScanSpec.getSerializedFilter());
      String host = regionsToScan.get(regionInfo).getHostname();
      Iterator<Integer> indexIterator = mapIterator.get(host);
      if (indexIterator == null) {
        indexIterator = nullIterator;
      }
      mappings.put(indexIterator.next(), p);
    }
  }

  @Override
  public HBaseSubScan getSpecificScan(int minorFragmentId) {
    return new HBaseSubScan(storagePlugin, storagePluginConfig, mappings.get(minorFragmentId), columns);
  }


  @Override
  public int getMaxParallelizationWidth() {
    return regionsToScan.size();
  }

  @Override
  public OperatorCost getCost() {
    //TODO Figure out how to properly calculate cost
    return new OperatorCost(regionsToScan.size(), // network
                            1,  // disk
                            1,  // memory
                            1); // cpu
  }

  @Override
  public Size getSize() {
    // TODO - this is wrong, need to populate correctly
    int rowCount = (hbaseScanSpec.getFilter() != null ? 5 : 10) * regionsToScan.size();
    int avgColumnSize = 10;
    int numColumns = (columns == null || columns.isEmpty()) ? 100 : columns.size();
    return new Size(rowCount, numColumns*avgColumnSize);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    //TODO return copy of self
    return this;
  }

  @JsonIgnore
  public HBaseStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  @JsonIgnore
  public Configuration getHBaseConf() {
    return getStorageConfig().getHBaseConf();
  }

  @JsonIgnore
  public String getTableName() {
    return getHBaseScanSpec().getTableName();
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "HBaseGroupScan [HBaseScanSpec="
        + hbaseScanSpec + ", columns="
        + columns + "]";
  }

  @JsonProperty("storage")
  public HBaseStoragePluginConfig getStorageConfig() {
    return this.storagePluginConfig;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty
  public HBaseScanSpec getHBaseScanSpec() {
    return hbaseScanSpec;
  }

  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @JsonIgnore
  public void setFilterPushedDown(boolean b) {
    this.filterPushedDown = true;
  }

  @JsonIgnore
  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

}
