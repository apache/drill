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

package org.apache.drill.exec.store.elasticsearch;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@JsonTypeName("elasticsearch-scan")
public class ElasticSearchGroupScan extends AbstractGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchGroupScan.class);

  protected Log comlogger = LogFactory.getLog(this.getClass());

  private final ElasticSearchStoragePlugin plugin;

  private final ElasticSearchPluginConfig storagePluginConfig;

  private final ElasticSearchScanSpec scanSpec;

  private final List<SchemaPath> columns;

  private Stopwatch watch;

  private boolean filterPushedDown = false;

  @JsonCreator
  public ElasticSearchGroupScan(@JsonProperty("usernName") String userName, @JsonProperty("elasticSearchSpec") ElasticSearchScanSpec scanSpec, @JsonProperty("storage") ElasticSearchPluginConfig storagePluginConfig, @JsonProperty("columns") List<SchemaPath> columns, @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this(userName, (ElasticSearchStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), scanSpec, columns);
  }

  public ElasticSearchGroupScan(String userName, ElasticSearchStoragePlugin plugin, ElasticSearchScanSpec scanSpec, List<SchemaPath> columns) {
    super(userName);
    this.plugin = plugin;
    this.storagePluginConfig = plugin.getConfig();
    this.scanSpec = scanSpec;
    this.columns = columns;
    this.watch = Stopwatch.createUnstarted();
    init();
  }

  public ElasticSearchGroupScan(ElasticSearchGroupScan that) {
    this(that, that.columns);
    this.filterPushedDown = that.filterPushedDown;
  }

  public ElasticSearchGroupScan(ElasticSearchGroupScan that, List<SchemaPath> columns) {
    this(that.getUserName(), that.plugin, that.scanSpec, columns);
    this.filterPushedDown = that.filterPushedDown;
  }

  @JsonProperty("elasticSearchSpec")
  public ElasticSearchScanSpec getScanSpec() {
    return scanSpec;
  }

  @JsonIgnore
  public ElasticSearchStoragePlugin getStoragePlugin() {
    return plugin;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonIgnore
  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  @JsonIgnore
  public void setFilterPushedDown(boolean filterPushedDown) {
    this.filterPushedDown = filterPushedDown;
  }

  private NavigableMap<PartitionDefinition, ServerHost> regionsToScan;

  private TableStatsCalculator statsCalculator;

  private long scanSizeInBytes = 0;

  private Map<Integer, List<ElasticSearchScanSpec>> endpointFragmentMapping;

  private static final Comparator<List<ElasticSearchScanSpec>> LIST_SIZE_COMPARATOR = new Comparator<List<ElasticSearchScanSpec>>() {
    @Override
    public int compare(List<ElasticSearchScanSpec> list1, List<ElasticSearchScanSpec> list2) {
      return list1.size() - list2.size();
    }
  };

  private static final Comparator<List<ElasticSearchScanSpec>> LIST_SIZE_COMPARATOR_REV = Collections.reverseOrder(LIST_SIZE_COMPARATOR);

  private void init() {
    // TODO: init whatever
    // 这里还进行预先期望在那个进程中进行处理

    logger.debug("Getting region locations");

    try {

      statsCalculator = new TableStatsCalculator(scanSpec, plugin.getConfig(), storagePluginConfig);

      regionsToScan = new TreeMap<PartitionDefinition, ServerHost>();

      Settings esCfg = new PropertiesSettings();
      List<PartitionDefinition> partitions = RestService.findPartitions(esCfg, comlogger);
      for (PartitionDefinition part : partitions) {

        // 这个region所处于的地址
        regionsToScan.put(part, new ServerHost(part.nodeIp));
        scanSizeInBytes += statsCalculator.getRegionSizeInBytes(part);

      }
    } catch (IOException e) {
      throw new DrillRuntimeException("Error getting region info for table: " + scanSpec.getIndexName(), e);
    }

  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    watch.reset();
    watch.start();
    Map<String, DrillbitEndpoint> endpointMap = new HashMap<String, DrillbitEndpoint>();
    for (DrillbitEndpoint ep : plugin.getContext().getBits()) {
      // 该集群有些些机器
      endpointMap.put(ep.getAddress(), ep);
    }

    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<DrillbitEndpoint, EndpointAffinity>();
    for (ServerHost server : regionsToScan.values()) {
      DrillbitEndpoint ep = endpointMap.get(server.getIp());
      if (ep != null) {
        EndpointAffinity affinity = affinityMap.get(ep);
        if (affinity == null) {
          affinityMap.put(ep, new EndpointAffinity(ep, 1));
        } else {
          affinity.addAffinity(1);
        }
      }
    }
    logger.debug("Took {} µs to get operator affinity", watch.elapsed(TimeUnit.NANOSECONDS) / 1000);
    // 这些进程使用有效的次数
    return Lists.newArrayList(affinityMap.values());
  }

  /**
   * @param incomingEndpoints
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    watch.reset();
    watch.start();

    final int numSlots = incomingEndpoints.size();
    Preconditions.checkArgument(numSlots <= regionsToScan.size(), String.format("Incoming endpoints %d is greater than number of scan regions %d", numSlots, regionsToScan.size()));

    /*
     * Minimum/Maximum number of assignment per slot
     */
    final int minPerEndpointSlot = (int) Math.floor((double) regionsToScan.size() / numSlots);
    final int maxPerEndpointSlot = (int) Math.ceil((double) regionsToScan.size() / numSlots);

    /*
     * initialize (endpoint index => HBaseSubScanSpec list) map
     */
    endpointFragmentMapping = Maps.newHashMapWithExpectedSize(numSlots);

    /*
     * another map with endpoint (hostname => corresponding index list) in
     * 'incomingEndpoints' list
     */
    Map<String, Queue<Integer>> endpointHostIndexListMap = Maps.newHashMap();

    /*
     * Initialize these two maps
     */
    // 那台机器负责那些slot，也就是那个进程要运行多少个任务，其实可以优化，同一机器优先执行本机器的请求
    for (int i = 0; i < numSlots; ++i) {
      endpointFragmentMapping.put(i, new ArrayList<ElasticSearchScanSpec>(maxPerEndpointSlot));
      String hostname = incomingEndpoints.get(i).getAddress();
      // hostname --> slot
      Queue<Integer> hostIndexQueue = endpointHostIndexListMap.get(hostname);
      if (hostIndexQueue == null) {
        hostIndexQueue = Lists.newLinkedList();
        endpointHostIndexListMap.put(hostname, hostIndexQueue);
      }
      hostIndexQueue.add(i);
    }
    // region --> hostname
    Set<Entry<PartitionDefinition, ServerHost>> regionsToAssignSet = Sets.newHashSet(regionsToScan.entrySet());

    /*
     * First, we assign regions which are hosted on region servers running
     * on drillbit endpoints
     */
    for (Iterator<Entry<PartitionDefinition, ServerHost>> regionsIterator = regionsToAssignSet.iterator(); regionsIterator.hasNext(); /* nothing */) {
      Entry<PartitionDefinition, ServerHost> regionEntry = regionsIterator.next();
      /*
       * Test if there is a drillbit endpoint which is also an HBase
       * RegionServer that hosts the current HBase region
       */
      // 同机器执行在这里进行了优化了
      Queue<Integer> endpointIndexlist = endpointHostIndexListMap.get(regionEntry.getValue().getIp());
      if (endpointIndexlist != null) {
        Integer slotIndex = endpointIndexlist.poll();
        List<ElasticSearchScanSpec> endpointSlotScanList = endpointFragmentMapping.get(slotIndex);
        // 对这个的业务进程的查询,这里同时生成查询条件的了
        endpointSlotScanList.add(regionInfoToSubScanSpec(regionEntry.getKey()));
        // add to the tail of the slot list, to add more later in round
        // robin fashion
        endpointIndexlist.offer(slotIndex);
        // this region has been assigned
        regionsIterator.remove();
      }
    }

    /*
     * Build priority queues of slots, with ones which has tasks lesser than
     * 'minPerEndpointSlot' and another which have more.
     */
    PriorityQueue<List<ElasticSearchScanSpec>> minHeap = new PriorityQueue<List<ElasticSearchScanSpec>>(numSlots, LIST_SIZE_COMPARATOR);
    PriorityQueue<List<ElasticSearchScanSpec>> maxHeap = new PriorityQueue<List<ElasticSearchScanSpec>>(numSlots, LIST_SIZE_COMPARATOR_REV);
    for (List<ElasticSearchScanSpec> listOfScan : endpointFragmentMapping.values()) {
      if (listOfScan.size() < minPerEndpointSlot) {
        // 分配任务小于平均值的
        minHeap.offer(listOfScan);
      } else if (listOfScan.size() > minPerEndpointSlot) {
        maxHeap.offer(listOfScan);
      }
    }

    /*
     * Now, let's process any regions which remain unassigned and assign
     * them to slots with minimum number of assignments.
     */
    // 还有些查询没有分配的
    if (regionsToAssignSet.size() > 0) {
      for (Entry<PartitionDefinition, ServerHost> regionEntry : regionsToAssignSet) {
        List<ElasticSearchScanSpec> smallestList = minHeap.poll();
        // 加到这个节点中
        smallestList.add(regionInfoToSubScanSpec(regionEntry.getKey()));
        if (smallestList.size() < maxPerEndpointSlot) {
          minHeap.offer(smallestList);
        }
      }
    }

    /*
     * While there are slots with lesser than 'minPerEndpointSlot' unit
     * work, balance from those with more.
     */
    while (minHeap.peek() != null && minHeap.peek().size() < minPerEndpointSlot) {
      List<ElasticSearchScanSpec> smallestList = minHeap.poll();
      List<ElasticSearchScanSpec> largestList = maxHeap.poll();
      smallestList.add(largestList.remove(largestList.size() - 1));
      if (largestList.size() > minPerEndpointSlot) {
        maxHeap.offer(largestList);
      }
      if (smallestList.size() < minPerEndpointSlot) {
        minHeap.offer(smallestList);
      }
    }

    /* no slot should be empty at this point */
    assert (minHeap.peek() == null || minHeap.peek().size() > 0) : String.format("Unable to assign tasks to some endpoints.\nEndpoints: {}.\nAssignment Map: {}.", incomingEndpoints, endpointFragmentMapping.toString());

    logger.debug("Built assignment map in {} µs.\nEndpoints: {}.\nAssignment Map: {}", watch.elapsed(TimeUnit.NANOSECONDS) / 1000, incomingEndpoints, endpointFragmentMapping.toString());
  }

  private ElasticSearchScanSpec regionInfoToSubScanSpec(PartitionDefinition part) {
    // 这里就生成查询对象了
    // HBaseScanSpec spec = hbaseScanSpec;
    // return new HBaseSubScanSpec()
    // .setTableName(spec.getTableName())
    // .setRegionServer(regionsToScan.get(ri).getHostname())
    // .setStartRow((!isNullOrEmpty(spec.getStartRow()) &&
    // ri.containsRow(spec.getStartRow())) ? spec.getStartRow() :
    // ri.getStartKey())
    // .setStopRow((!isNullOrEmpty(spec.getStopRow()) &&
    // ri.containsRow(spec.getStopRow())) ? spec.getStopRow() :
    // ri.getEndKey())
    // .setSerializedFilter(spec.getSerializedFilter());

    ElasticSearchScanSpec spec = new ElasticSearchScanSpec(scanSpec.getIndexName(), scanSpec.getTypeMappingName(), part);

    return spec;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    // TODO: What is minor fragmentation id ?
    // it shoud add many watch here
    // return new ElasticSearchSubScan(super.getUserName(), this.plugin,
    // this.storagePluginConfig, this.scanSpec, this.columns);

    assert minorFragmentId < endpointFragmentMapping.size() : String.format("Mappings length [%d] should be greater than minor fragment id [%d] but it isn't.", endpointFragmentMapping.size(), minorFragmentId);
    // 之前已经这样分配结果的了
    // return new HBaseSubScan(getUserName(), storagePlugin,
    // storagePluginConfig,
    // endpointFragmentMapping.get(minorFragmentId), columns);
    List<ElasticSearchScanSpec> specs = endpointFragmentMapping.get(minorFragmentId);
    return new ElasticSearchSubScan(super.getUserName(), this.plugin, this.storagePluginConfig, specs, this.columns);

  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new ElasticSearchGroupScan(this);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    ElasticSearchGroupScan clone = new ElasticSearchGroupScan(this, columns);
    return clone;
  }

  @Override
  public ScanStats getScanStats() {
    // 拉取统计数据
    Response response;
    JsonNode jsonNode;
    RestClient client = this.plugin.getClient();
    try {
      response = client.performRequest("GET", "/" + this.scanSpec.getIndexName() + "/" + this.scanSpec.getTypeMappingName() + "/_count");
      jsonNode = JsonHelper.readRespondeContentAsJsonTree(this.plugin.getObjectMapper(), response);
      // 拿到统计量
      JsonNode countNode = JsonHelper.getPath(jsonNode, "count");
      long numDocs = 0;
      if (!countNode.isMissingNode()) {
        numDocs = countNode.longValue();
      } else {
        logger.warn("There are no documents in {}.{}?", this.scanSpec.getIndexName(), this.scanSpec.getTypeMappingName());
      }
      long docSize = 0;
      if (numDocs > 0) {
        response = client.performRequest("GET", "/" + this.scanSpec.getIndexName() + "/" + this.scanSpec.getTypeMappingName() + "/_search?size=1&terminate_after=1");
        jsonNode = JsonHelper.readRespondeContentAsJsonTree(this.plugin.getObjectMapper(), response);
        JsonNode hits = JsonHelper.getPath(jsonNode, "hits.hits");
        if (!hits.isMissingNode()) {
          // TODO: Is there another elegant way to get the JsonNode
          // Content?
          // 拿一条数据出来
          docSize = hits.elements().next().toString().getBytes().length;
        } else {
          throw new DrillRuntimeException("Couldn't size any documents for " + this.scanSpec.getIndexName() + "." + this.scanSpec.getTypeMappingName());
        }
      }
      // 这样就知道有多少数据量了
      return new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, numDocs, 1, docSize * numDocs);
    } catch (IOException e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }
}
