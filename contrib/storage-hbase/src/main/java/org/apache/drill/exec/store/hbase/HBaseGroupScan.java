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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.hbase.HBaseSubScan.HBaseSubScanSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;

import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@JsonTypeName("hbase-scan")
public class HBaseGroupScan extends AbstractGroupScan implements DrillHBaseConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseGroupScan.class);

  private static final Comparator<List<HBaseSubScanSpec>> LIST_SIZE_COMPARATOR = new Comparator<List<HBaseSubScanSpec>>() {
    @Override
    public int compare(List<HBaseSubScanSpec> list1, List<HBaseSubScanSpec> list2) {
      return list1.size() - list2.size();
    }
  };

  private static final Comparator<List<HBaseSubScanSpec>> LIST_SIZE_COMPARATOR_REV = Collections.reverseOrder(LIST_SIZE_COMPARATOR);

  private HBaseStoragePluginConfig storagePluginConfig;

  private List<SchemaPath> columns;

  private HBaseScanSpec hbaseScanSpec;

  private HBaseStoragePlugin storagePlugin;

  private Stopwatch watch = new Stopwatch();

  private Map<Integer, List<HBaseSubScanSpec>> endpointFragmentMapping;

  private NavigableMap<HRegionInfo, ServerName> regionsToScan;

  private HTableDescriptor hTableDesc;

  private boolean filterPushedDown = false;

  private TableStatsCalculator statsCalculator;

  private long scanSizeInBytes = 0;

  @JsonCreator
  public HBaseGroupScan(@JsonProperty("hbaseScanSpec") HBaseScanSpec hbaseScanSpec,
                        @JsonProperty("storage") HBaseStoragePluginConfig storagePluginConfig,
                        @JsonProperty("columns") List<SchemaPath> columns,
                        @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this ((HBaseStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), hbaseScanSpec, columns);
  }

  public HBaseGroupScan(HBaseStoragePlugin storagePlugin, HBaseScanSpec scanSpec, List<SchemaPath> columns) {
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.hbaseScanSpec = scanSpec;
    this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;
    init();
  }

  /**
   * Private constructor, used for cloning.
   * @param that The HBaseGroupScan to clone
   */
  private HBaseGroupScan(HBaseGroupScan that) {
    this.columns = that.columns;
    this.hbaseScanSpec = that.hbaseScanSpec;
    this.endpointFragmentMapping = that.endpointFragmentMapping;
    this.regionsToScan = that.regionsToScan;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.hTableDesc = that.hTableDesc;
    this.filterPushedDown = that.filterPushedDown;
    this.statsCalculator = that.statsCalculator;
    this.scanSizeInBytes = that.scanSizeInBytes;
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
      statsCalculator = new TableStatsCalculator(table, hbaseScanSpec, storagePlugin.getContext().getConfig());

      boolean foundStartRegion = false;
      regionsToScan = new TreeMap<HRegionInfo, ServerName>();
      for (Entry<HRegionInfo, ServerName> mapEntry : regionsMap.entrySet()) {
        HRegionInfo regionInfo = mapEntry.getKey();
        if (!foundStartRegion && hbaseScanSpec.getStartRow() != null && hbaseScanSpec.getStartRow().length != 0 && !regionInfo.containsRow(hbaseScanSpec.getStartRow())) {
          continue;
        }
        foundStartRegion = true;
        regionsToScan.put(regionInfo, mapEntry.getValue());
        scanSizeInBytes += statsCalculator.getRegionSizeInBytes(regionInfo.getRegionName());
        if (hbaseScanSpec.getStopRow() != null && hbaseScanSpec.getStopRow().length != 0 && regionInfo.containsRow(hbaseScanSpec.getStopRow())) {
          break;
        }
      }

      table.close();
    } catch (IOException e) {
      throw new DrillRuntimeException("Error getting region info for table: " + hbaseScanSpec.getTableName(), e);
    }
    verifyColumns();
  }

  private void verifyColumns() {
    if (AbstractRecordReader.isStarQuery(columns)) return;
    for (SchemaPath column : columns) {
      if (!(column.equals(ROW_KEY_PATH) || hTableDesc.hasFamily(HBaseUtils.getBytes(column.getRootSegment().getPath())))) {
        DrillRuntimeException.format("The column family '%s' does not exist in HBase table: %s .",
            column.getRootSegment().getPath(), hTableDesc.getNameAsString());
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
      DrillbitEndpoint ep = endpointMap.get(sn.getHostname());
      if (ep != null) {
        EndpointAffinity affinity = affinityMap.get(ep);
        if (affinity == null) {
          affinityMap.put(ep, new EndpointAffinity(ep, 1));
        } else {
          affinity.addAffinity(1);
        }
      }
    }
    logger.debug("Took {} µs to get operator affinity", watch.elapsed(TimeUnit.NANOSECONDS)/1000);
    return Lists.newArrayList(affinityMap.values());
  }

  /**
   *
   * @param incomingEndpoints
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    watch.reset();
    watch.start();

    final int numSlots = incomingEndpoints.size();
    Preconditions.checkArgument(numSlots <= regionsToScan.size(),
        String.format("Incoming endpoints %d is greater than number of scan regions %d", numSlots, regionsToScan.size()));

    /*
     * Minimum/Maximum number of assignment per slot
     */
    final int minPerEndpointSlot = (int) Math.floor((double)regionsToScan.size() / numSlots);
    final int maxPerEndpointSlot = (int) Math.ceil((double)regionsToScan.size() / numSlots);

    /*
     * initialize (endpoint index => HBaseSubScanSpec list) map
     */
    endpointFragmentMapping = Maps.newHashMapWithExpectedSize(numSlots);

    /*
     * another map with endpoint (hostname => corresponding index list) in 'incomingEndpoints' list
     */
    Map<String, Queue<Integer>> endpointHostIndexListMap = Maps.newHashMap();

    /*
     * Initialize these two maps
     */
    for (int i = 0; i < numSlots; ++i) {
      endpointFragmentMapping.put(i, new ArrayList<HBaseSubScanSpec>(maxPerEndpointSlot));
      String hostname = incomingEndpoints.get(i).getAddress();
      Queue<Integer> hostIndexQueue = endpointHostIndexListMap.get(hostname);
      if (hostIndexQueue == null) {
        hostIndexQueue = Lists.newLinkedList();
        endpointHostIndexListMap.put(hostname, hostIndexQueue);
      }
      hostIndexQueue.add(i);
    }

    Set<Entry<HRegionInfo, ServerName>> regionsToAssignSet = Sets.newHashSet(regionsToScan.entrySet());

    /*
     * First, we assign regions which are hosted on region servers running on drillbit endpoints
     */
    for (Iterator<Entry<HRegionInfo, ServerName>> regionsIterator = regionsToAssignSet.iterator(); regionsIterator.hasNext(); /*nothing*/) {
      Entry<HRegionInfo, ServerName> regionEntry = regionsIterator.next();
      /*
       * Test if there is a drillbit endpoint which is also an HBase RegionServer that hosts the current HBase region
       */
      Queue<Integer> endpointIndexlist = endpointHostIndexListMap.get(regionEntry.getValue().getHostname());
      if (endpointIndexlist != null) {
        Integer slotIndex = endpointIndexlist.poll();
        List<HBaseSubScanSpec> endpointSlotScanList = endpointFragmentMapping.get(slotIndex);
        endpointSlotScanList.add(regionInfoToSubScanSpec(regionEntry.getKey()));
        // add to the tail of the slot list, to add more later in round robin fashion
        endpointIndexlist.offer(slotIndex);
        // this region has been assigned
        regionsIterator.remove();
      }
    }

    /*
     * Build priority queues of slots, with ones which has tasks lesser than 'minPerEndpointSlot' and another which have more.
     */
    PriorityQueue<List<HBaseSubScanSpec>> minHeap = new PriorityQueue<List<HBaseSubScanSpec>>(numSlots, LIST_SIZE_COMPARATOR);
    PriorityQueue<List<HBaseSubScanSpec>> maxHeap = new PriorityQueue<List<HBaseSubScanSpec>>(numSlots, LIST_SIZE_COMPARATOR_REV);
    for(List<HBaseSubScanSpec> listOfScan : endpointFragmentMapping.values()) {
      if (listOfScan.size() < minPerEndpointSlot) {
        minHeap.offer(listOfScan);
      } else if (listOfScan.size() > minPerEndpointSlot){
        maxHeap.offer(listOfScan);
      }
    }

    /*
     * Now, let's process any regions which remain unassigned and assign them to slots with minimum number of assignments.
     */
    if (regionsToAssignSet.size() > 0) {
      for (Entry<HRegionInfo, ServerName> regionEntry : regionsToAssignSet) {
        List<HBaseSubScanSpec> smallestList = minHeap.poll();
        smallestList.add(regionInfoToSubScanSpec(regionEntry.getKey()));
        if (smallestList.size() < minPerEndpointSlot) {
          minHeap.offer(smallestList);
        }
      }
    }

    /*
     * While there are slots with lesser than 'minPerEndpointSlot' unit work, balance from those with more.
     */
    while(minHeap.peek() != null && minHeap.peek().size() < minPerEndpointSlot) {
      List<HBaseSubScanSpec> smallestList = minHeap.poll();
      List<HBaseSubScanSpec> largestList = maxHeap.poll();
      smallestList.add(largestList.remove(largestList.size()-1));
      if (largestList.size() > minPerEndpointSlot) {
        maxHeap.offer(largestList);
      }
      if (smallestList.size() < minPerEndpointSlot) {
        minHeap.offer(smallestList);
      }
    }

    /* no slot should be empty at this point */
    assert (minHeap.peek() == null || minHeap.peek().size() > 0) : String.format(
        "Unable to assign tasks to some endpoints.\nEndpoints: {}.\nAssignment Map: {}.",
        incomingEndpoints, endpointFragmentMapping.toString());

    logger.debug("Built assignment map in {} µs.\nEndpoints: {}.\nAssignment Map: {}",
        watch.elapsed(TimeUnit.NANOSECONDS)/1000, incomingEndpoints, endpointFragmentMapping.toString());
  }

  private HBaseSubScanSpec regionInfoToSubScanSpec(HRegionInfo ri) {
    HBaseScanSpec spec = hbaseScanSpec;
    return new HBaseSubScanSpec()
        .setTableName(spec.getTableName())
        .setRegionServer(regionsToScan.get(ri).getHostname())
        .setStartRow((!isNullOrEmpty(spec.getStartRow()) && ri.containsRow(spec.getStartRow())) ? spec.getStartRow() : ri.getStartKey())
        .setStopRow((!isNullOrEmpty(spec.getStopRow()) && ri.containsRow(spec.getStopRow())) ? spec.getStopRow() : ri.getEndKey())
        .setSerializedFilter(spec.getSerializedFilter());
  }

  private boolean isNullOrEmpty(byte[] key) {
    return key == null || key.length == 0;
  }

  @Override
  public HBaseSubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < endpointFragmentMapping.size() : String.format(
        "Mappings length [%d] should be greater than minor fragment id [%d] but it isn't.", endpointFragmentMapping.size(),
        minorFragmentId);
    return new HBaseSubScan(storagePlugin, storagePluginConfig, endpointFragmentMapping.get(minorFragmentId), columns);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return regionsToScan.size();
  }

  @Override
  public ScanStats getScanStats() {
    int rowCount =  (int) ((scanSizeInBytes / statsCalculator.getAvgRowSizeInBytes()) * (hbaseScanSpec.getFilter() != null ? 0.5 : 1));
    // the following calculation is not precise since 'columns' could specify CFs while getColsPerRow() returns the number of qualifier.
    float diskCost = scanSizeInBytes * ((columns == null || columns.isEmpty()) ? 1 : columns.size()/statsCalculator.getColsPerRow());
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, rowCount, 1, diskCost);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HBaseGroupScan(this);
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

  /**
   * Empty constructor, do not use, only for testing.
   */
  @VisibleForTesting
  public HBaseGroupScan() { }

  /**
   * Do not use, only for testing.
   */
  @VisibleForTesting
  public void setHBaseScanSpec(HBaseScanSpec hbaseScanSpec) {
    this.hbaseScanSpec = hbaseScanSpec;
  }

  /**
   * Do not use, only for testing.
   */
  @JsonIgnore
  @VisibleForTesting
  public void setRegionsToScan(NavigableMap<HRegionInfo, ServerName> regionsToScan) {
    this.regionsToScan = regionsToScan;
  }

}
