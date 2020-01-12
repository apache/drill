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
import java.net.URL;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.PartitionDefinition;
import org.elasticsearch.hadoop.rest.RestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_PORT;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_NODES;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_NODES_DISCOVERY;


@JsonTypeName("elasticsearch-scan")
public class ElasticSearchGroupScan extends AbstractGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchGroupScan.class);

  protected Log comlogger = LogFactory.getLog(this.getClass());

  private final ElasticSearchStoragePlugin plugin;

  private final ElasticSearchPluginConfig storagePluginConfig;

  private final ElasticSearchScanSpec scanSpec;

  private final List<SchemaPath> columns;

  private boolean filterPushedDown = false;

  private NavigableMap<PartitionDefinition, ServerHost> regionsToScan;

  private long scanSizeInBytes = 0;

  private Map<Integer, List<ElasticSearchScanSpec>> endpointFragmentMapping;

  private static final Comparator<List<ElasticSearchScanSpec>> LIST_SIZE_COMPARATOR = new Comparator<List<ElasticSearchScanSpec>>() {
    @Override
    public int compare(List<ElasticSearchScanSpec> list1, List<ElasticSearchScanSpec> list2) {
      return list1.size() - list2.size();
    }
  };

  private static final Comparator<List<ElasticSearchScanSpec>> LIST_SIZE_COMPARATOR_REV = Collections.reverseOrder(LIST_SIZE_COMPARATOR);

  @JsonCreator
  public ElasticSearchGroupScan(@JsonProperty("usernName") String userName,
                                @JsonProperty("elasticSearchSpec") ElasticSearchScanSpec scanSpec,
                                @JsonProperty("storage") ElasticSearchPluginConfig storagePluginConfig,
                                @JsonProperty("columns") List<SchemaPath> columns,
                                @JacksonInject StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    this(userName, (ElasticSearchStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), scanSpec, columns);
  }

  public ElasticSearchGroupScan(String userName, ElasticSearchStoragePlugin plugin, ElasticSearchScanSpec scanSpec, List<SchemaPath> columns) {
    super(userName);
    this.plugin = plugin;
    this.scanSpec = scanSpec;
    this.columns = columns;
    storagePluginConfig = plugin.getConfig();
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


  private void init() {
    // Here is also expected to process in that process in advance
    logger.debug("Getting region locations");

    try {

      TableStatsCalculator statsCalculator = new TableStatsCalculator(storagePluginConfig);

      regionsToScan = new TreeMap<>();

      // Get the connection from the plugin config
      URL parsedHostsAndPorts = new URL(storagePluginConfig.getHostsAndPorts());
      int port = parsedHostsAndPorts.getPort();
      String host = parsedHostsAndPorts.getHost();

      // Add root to Properties
      // Example code from https://github.com/elastic/elasticsearch-hadoop/blob/master/mr/src/test/java/org/elasticsearch/hadoop/util/SettingsUtilsTest.java

      String index = scanSpec.getIndexName();
      String mappingName = scanSpec.getTypeMappingName();
      String resource = index + "/" + mappingName;

      // TODO Add ES Config Options in Plugin Config

      Settings settings = new PropertiesSettings();
      settings.setMaxDocsPerPartition(10000);
      settings.setProperty(ES_NODES, host);
      settings.setProperty(ES_PORT, String.valueOf(port));
      settings.setProperty(ES_NODES_DISCOVERY, "false");
      settings.setProperty(ES_RESOURCE_READ, resource);
      settings.setProperty("es.nodes.wan.only", "true");


      List<PartitionDefinition> partitions = RestService.findPartitions(settings, comlogger);

      for (PartitionDefinition part : partitions) {
        // The address of this region

        if (part.getLocations().length == 0) {
          regionsToScan.put(part, new ServerHost(parsedHostsAndPorts.toString()));
          part.settings().setNodes(parsedHostsAndPorts.toString());
        } else {
          for (String ip : part.getLocations()) {
            logger.debug("Adding ip: {}", ip);
            regionsToScan.put(part, new ServerHost(ip));
          }
        }
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
    Map<String, DrillbitEndpoint> endpointMap = new HashMap<>();
    for (DrillbitEndpoint ep : plugin.getContext().getBits()) {
      // The cluster has some machines
      endpointMap.put(ep.getAddress(), ep);
    }

    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<>();
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
    // The cluster has some machines
    return Lists.newArrayList(affinityMap.values());
  }

  /**
   * Applies assignments to a list of ElasticSearch endpoints.
   * @param incomingEndpoints list of incoming endpoints
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {

    int numSlots = incomingEndpoints.size();
    Preconditions.checkArgument(numSlots <= regionsToScan.size(), String.format("Incoming endpoints %d is greater than number of scan regions %d", numSlots, regionsToScan.size()));

    /*
     * Minimum/Maximum number of assignment per slot
     */
    int minPerEndpointSlot = (int) Math.floor((double) regionsToScan.size() / numSlots);
    int maxPerEndpointSlot = (int) Math.ceil((double) regionsToScan.size() / numSlots);

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
    // That machine is responsible for those slots, that is, how many tasks the process will run. In fact, it can be optimized. The same machine preferentially executes the requests of this machine.
    for (int i = 0; i < numSlots; ++i) {
      endpointFragmentMapping.put(i, new ArrayList<>(maxPerEndpointSlot));
      String hostname = incomingEndpoints.get(i).getAddress();
      // hostname --> slot
      Queue<Integer> hostIndexQueue = endpointHostIndexListMap.computeIfAbsent(hostname, k -> Lists.newLinkedList());
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
      // The same machine execution is optimized here
      Queue<Integer> endpointIndexlist = endpointHostIndexListMap.get(regionEntry.getValue().getIp());
      if (endpointIndexlist != null) {
        Integer slotIndex = endpointIndexlist.poll();
        List<ElasticSearchScanSpec> endpointSlotScanList = endpointFragmentMapping.get(slotIndex);
        // For the query of this business process, the query conditions are also generated here.
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
    PriorityQueue<List<ElasticSearchScanSpec>> minHeap = new PriorityQueue<>(numSlots, LIST_SIZE_COMPARATOR);
    PriorityQueue<List<ElasticSearchScanSpec>> maxHeap = new PriorityQueue<>(numSlots, LIST_SIZE_COMPARATOR_REV);
    for (List<ElasticSearchScanSpec> listOfScan : endpointFragmentMapping.values()) {
      if (listOfScan.size() < minPerEndpointSlot) {
        // Assignment task is less than average
        minHeap.offer(listOfScan);
      } else if (listOfScan.size() > minPerEndpointSlot) {
        maxHeap.offer(listOfScan);
      }
    }

    /*
     * Now, let's process any regions which remain unassigned and assign
     * them to slots with minimum number of assignments.
     */
    // Some queries are not assigned
    if (regionsToAssignSet.size() > 0) {
      for (Entry<PartitionDefinition, ServerHost> regionEntry : regionsToAssignSet) {
        List<ElasticSearchScanSpec> smallestList = minHeap.poll();

        // Add to this node
        assert smallestList != null;
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

      assert largestList != null;
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
  }

  private ElasticSearchScanSpec regionInfoToSubScanSpec(PartitionDefinition part) {
    // The query object is generated here
    return new ElasticSearchScanSpec(scanSpec.getIndexName(), scanSpec.getTypeMappingName(), part);
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {

    assert minorFragmentId < endpointFragmentMapping.size() : String.format("Mappings length [%d] should be greater than minor fragment id [%d] but it isn't.",
      endpointFragmentMapping.size(),
      minorFragmentId);

    // The results have been distributed before
    List<ElasticSearchScanSpec> specs = endpointFragmentMapping.get(minorFragmentId);
    return new ElasticSearchSubScan(super.getUserName(), plugin, storagePluginConfig, specs, columns);
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
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ElasticSearchGroupScan(this);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return new ElasticSearchGroupScan(this, columns);
  }

  @Override
  public ScanStats getScanStats() {
    // Pull statistics
    Response response;
    JsonNode jsonNode;
    RestClient client = plugin.getClient();
    try {
      response = client.performRequest(new Request("GET", "/" + scanSpec.getIndexName() + "/" + scanSpec.getTypeMappingName() + "/_count"));
      logger.debug("Making GET request: {}", "/" + scanSpec.getIndexName() + "/" + scanSpec.getTypeMappingName() + "/_count");

      jsonNode = JsonHelper.readResponseContentAsJsonTree(plugin.getObjectMapper(), response);
      // Get statistics
      JsonNode countNode = JsonHelper.getPath(jsonNode, "count");
      long numDocs = 0;
      if (!countNode.isMissingNode()) {
        numDocs = countNode.longValue();
      } else {
        logger.warn("There are no documents in {}.{}?", scanSpec.getIndexName(), scanSpec.getTypeMappingName());
      }
      long docSize = 0;
      if (numDocs > 0) {
        response = client.performRequest(new Request("GET", "/" + scanSpec.getIndexName() + "/" + scanSpec.getTypeMappingName() + "/_search?size=1&terminate_after=1"));
        logger.debug("Making GET request: {}", "/" + scanSpec.getIndexName() + "/" + scanSpec.getTypeMappingName() + "/_search?size=1&terminate_after=1");

        jsonNode = JsonHelper.readResponseContentAsJsonTree(plugin.getObjectMapper(), response);
        JsonNode hits = JsonHelper.getPath(jsonNode, "hits.hits");
        if (!hits.isMissingNode()) {
          // Take a piece of data
          docSize = hits
            .elements()
            .next()
            .toString()
            .getBytes()
            .length;
        } else {
          throw UserException
            .resourceError()
            .message("Couldn't size any documents for " + scanSpec.getIndexName() + "." + scanSpec.getTypeMappingName())
            .build(logger);
        }
      }
      // So you know how much data you have
      return new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, numDocs, 1, docSize * numDocs);
    } catch (IOException e) {
      throw UserException
        .resourceError()
        .message(e.getMessage())
        .build(logger);
    }
  }
}
