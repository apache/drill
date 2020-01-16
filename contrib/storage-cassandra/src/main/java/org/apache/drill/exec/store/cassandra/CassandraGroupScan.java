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

package org.apache.drill.exec.store.cassandra;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.cassandra.CassandraSubScan.CassandraSubScanSpec;

import org.apache.drill.exec.store.cassandra.connection.CassandraConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;

@JsonTypeName("cassandra-scan")
public class CassandraGroupScan extends AbstractGroupScan implements DrillCassandraConstants {
  private static final Logger logger = LoggerFactory.getLogger(CassandraGroupScan.class);

  private static final Comparator<List<CassandraSubScanSpec>> LIST_SIZE_COMPARATOR = new Comparator<List<CassandraSubScanSpec>>() {
    @Override
    public int compare(List<CassandraSubScanSpec> list1, List<CassandraSubScanSpec> list2) {
      return list1.size() - list2.size();
    }
  };

  private static final Comparator<List<CassandraSubScanSpec>> LIST_SIZE_COMPARATOR_REV = Collections.reverseOrder(LIST_SIZE_COMPARATOR);

  private String userName;

  private CassandraStoragePluginConfig storagePluginConfig;

  private List<SchemaPath> columns;

  private CassandraScanSpec cassandraScanSpec;

  private CassandraStoragePlugin storagePlugin;

  private Stopwatch watch = Stopwatch.createUnstarted();

  private Map<Integer, List<CassandraSubScanSpec>> endpointFragmentMapping;

  private Set<Host> keyspaceHosts;

  private int totalAssignmentsTobeDone;

  private boolean filterPushedDown = false;

  private Map<String, CassandraPartitionToken> hostTokenMapping = new HashMap<>();

  //private TableStatsCalculator statsCalculator;

  private long scanSizeInBytes = 0;

  private Metadata metadata;

  private Cluster cluster;

  private Session session;

  private ResultSet rs;

  @JsonCreator
  public CassandraGroupScan(@JsonProperty("userName") String userName,
                            @JsonProperty("cassandraScanSpec") CassandraScanSpec cassandraScanSpec,
                            @JsonProperty("storage") CassandraStoragePluginConfig storagePluginConfig,
                            @JsonProperty("columns") List<SchemaPath> columns,
                            @JacksonInject StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    this(userName, (CassandraStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), cassandraScanSpec, columns);
  }

  public CassandraGroupScan(String userName, CassandraStoragePlugin storagePlugin, CassandraScanSpec scanSpec, List<SchemaPath> columns) {
    super(userName);
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.cassandraScanSpec = scanSpec;
    this.columns = columns == null || columns.size() == 0 ? ALL_COLUMNS : columns;
    init();
  }

  /**
   * Private constructor, used for cloning.
   *
   * @param that The CassandraGroupScan to clone
   */
  private CassandraGroupScan(CassandraGroupScan that) {
    super(that);
    this.columns = that.columns;
    this.cassandraScanSpec = that.cassandraScanSpec;
    this.endpointFragmentMapping = that.endpointFragmentMapping;
    this.keyspaceHosts = that.keyspaceHosts;
    this.metadata = that.metadata;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.filterPushedDown = that.filterPushedDown;
    this.totalAssignmentsTobeDone = that.totalAssignmentsTobeDone;
    this.hostTokenMapping = that.hostTokenMapping;
    //this.statsCalculator = that.statsCalculator;
    this.scanSizeInBytes = that.scanSizeInBytes;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    CassandraGroupScan newScan = new CassandraGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  private void init() {
    try {
      logger.debug(String.format("Getting cassandra session from host %s, port: %s.", storagePluginConfig.getHosts(), storagePluginConfig.getPort()));

      cluster = CassandraConnectionManager.getCluster(storagePluginConfig.getHosts(), storagePluginConfig.getPort());

      session = cluster.connect();

      metadata = session.getCluster().getMetadata();

      Charset charset = Charset.forName("UTF-8");
      CharsetEncoder encoder = charset.newEncoder();

      keyspaceHosts = session.getCluster().getMetadata().getAllHosts();

      logger.debug("KeySpace hosts for Cassandra : {}", keyspaceHosts);

      if (null == keyspaceHosts) {
        logger.error(String.format("No Keyspace Hosts Found for Cassandra %s:%s .", storagePluginConfig.getHosts(), storagePluginConfig.getPort()));
        throw new DrillRuntimeException(String.format("No Keyspace Hosts Found for Cassandra %s:%s .", storagePluginConfig.getHosts(), storagePluginConfig.getPort()));
      }

      String[] tokens = CassandraUtil.getPartitionTokens(metadata.getPartitioner(), keyspaceHosts.size());
      int index = 0;
      for (Host h : keyspaceHosts) {
        CassandraPartitionToken token = new CassandraPartitionToken();
        token.setLow(tokens[index]);
        if (index + 1 < tokens.length) {
          token.setHigh(tokens[index + 1]);
        }
        hostTokenMapping.put(h.getAddress().getHostName(), token);
        index++;
      }
      logger.debug("Host token mapping: {}", hostTokenMapping);

      Statement q = QueryBuilder.select().all().from(cassandraScanSpec.getKeyspace(), cassandraScanSpec.getTable());

      if (session.isClosed()) {
        logger.error("Error in initializing CasandraGroupScan. Session Closed.");
        throw new DrillRuntimeException("Error in initializing CasandraGroupScan. Session Closed.");
      }

      rs = session.execute(q);
      this.totalAssignmentsTobeDone = rs.getAvailableWithoutFetching();

    } catch (Exception e) {
      logger.error("Error in initializing CasandraGroupScan, Error: " + e.getMessage());
      throw new DrillRuntimeException(e);
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

    logger.debug("Building affinity map. Endpoints: {}, KeyspaceHosts: {}", endpointMap, keyspaceHosts);

    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<DrillbitEndpoint, EndpointAffinity>();
    for (Host host : keyspaceHosts) {
      DrillbitEndpoint ep = endpointMap.get(host.getAddress().getHostName());
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

    if (null == affinityMap || affinityMap.size() == 0) {
      logger.debug("Affinity map is empty for CassandraGroupScan {}:{}", storagePluginConfig.getHosts(), storagePluginConfig.getPort());
    }
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

    Preconditions.checkArgument(numSlots <= totalAssignmentsTobeDone, String.format("Incoming endpoints %d is greater than number of chunks %d", numSlots, totalAssignmentsTobeDone));

    final int minPerEndpointSlot = (int) Math.floor((double) totalAssignmentsTobeDone / numSlots);
    final int maxPerEndpointSlot = (int) Math.ceil((double) totalAssignmentsTobeDone / numSlots);

    /* Map for (index,endpoint)'s */
    endpointFragmentMapping = Maps.newHashMapWithExpectedSize(numSlots);
    /* Reverse mapping for above indexes */
    Map<String, Queue<Integer>> endpointHostIndexListMap = Maps.newHashMap();

    /*
     * Initialize these two maps
     */
    for (int i = 0; i < numSlots; ++i) {
      endpointFragmentMapping.put(i, new ArrayList<>(maxPerEndpointSlot));
      String hostname = incomingEndpoints.get(i).getAddress();
      Queue<Integer> hostIndexQueue = endpointHostIndexListMap.get(hostname);
      if (hostIndexQueue == null) {
        hostIndexQueue = Lists.newLinkedList();
        endpointHostIndexListMap.put(hostname, hostIndexQueue);
      }
      hostIndexQueue.add(i);
    }

    Set<Host> hostsToAssignSet = Sets.newHashSet(keyspaceHosts);

    for (Iterator<Host> hostIterator = hostsToAssignSet.iterator(); hostIterator.hasNext(); /*nothing*/) {
      Host hostEntry = hostIterator.next();

      Queue<Integer> endpointIndexlist = endpointHostIndexListMap.get(hostEntry.getAddress().getHostName());
      if (endpointIndexlist != null) {
        Integer slotIndex = endpointIndexlist.poll();
        List<CassandraSubScanSpec> endpointSlotScanList = endpointFragmentMapping.get(slotIndex);
        endpointSlotScanList.add(hostToSubScanSpec(hostEntry, storagePluginConfig.getHosts()));
        // add to the tail of the slot list, to add more later in round robin fashion
        endpointIndexlist.offer(slotIndex);
        // this region has been assigned
        hostIterator.remove();
      }
    }

    /*
     * Build priority queues of slots, with ones which has tasks lesser than 'minPerEndpointSlot' and another which have more.
     */
    PriorityQueue<List<CassandraSubScanSpec>> minHeap = new PriorityQueue<List<CassandraSubScanSpec>>(numSlots, LIST_SIZE_COMPARATOR);
    PriorityQueue<List<CassandraSubScanSpec>> maxHeap = new PriorityQueue<List<CassandraSubScanSpec>>(numSlots, LIST_SIZE_COMPARATOR_REV);
    for (List<CassandraSubScanSpec> listOfScan : endpointFragmentMapping.values()) {
      if (listOfScan.size() < minPerEndpointSlot) {
        minHeap.offer(listOfScan);
      } else if (listOfScan.size() > minPerEndpointSlot) {
        maxHeap.offer(listOfScan);
      }
    }

    /*
     * Now, let's process any regions which remain unassigned and assign them to slots with minimum number of assignments.
     */
    if (hostsToAssignSet.size() > 0) {
      for (Host hostEntry : hostsToAssignSet) {
        List<CassandraSubScanSpec> smallestList = minHeap.poll();
        smallestList.add(hostToSubScanSpec(hostEntry, storagePluginConfig.getHosts()));
        if (smallestList.size() < maxPerEndpointSlot) {
          minHeap.offer(smallestList);
        }
      }
    }

    /*
     * While there are slots with lesser than 'minPerEndpointSlot' unit work, balance from those with more.
     */
    try {
      // If there is more work left
      if (maxHeap.peek() != null && maxHeap.peek().size() > 0) {
        while (minHeap.peek() != null && minHeap.peek().size() <= minPerEndpointSlot) {
          List<CassandraSubScanSpec> smallestList = minHeap.poll();
          List<CassandraSubScanSpec> largestList = maxHeap.poll();

          smallestList.add(largestList.remove(largestList.size() - 1));
          if (largestList.size() > minPerEndpointSlot) {
            maxHeap.offer(largestList);
          }
          if (smallestList.size() <= minPerEndpointSlot) {
            minHeap.offer(smallestList);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }


    /* no slot should be empty at this point */
    assert (minHeap.peek() == null || minHeap.peek().size() > 0) : String.format("Unable to assign tasks to some endpoints.\nEndpoints: {}.\nAssignment Map: {}.", incomingEndpoints, endpointFragmentMapping.toString());

    logger.debug("Built assignment map in {} µs.\nEndpoints: {}.\nAssignment Map: {}", watch.elapsed(TimeUnit.NANOSECONDS) / 1000, incomingEndpoints, endpointFragmentMapping.toString());

  }

  private CassandraSubScanSpec hostToSubScanSpec(Host host, List<String> contactPoints) {
    CassandraScanSpec spec = cassandraScanSpec;
    CassandraPartitionToken token = hostTokenMapping.get(host.getAddress().getHostName());

    return new CassandraSubScanSpec()
      .setTable(spec.getTable())
      .setKeyspace(spec.getKeyspace())
      .setFilter(spec.getFilters())
      .setHosts(contactPoints)
      .setPort(storagePluginConfig.getPort())
      .setStartToken(token != null ? token.getLow() : null)
      .setEndToken(token != null ? token.getHigh() : null);

  }

  private boolean isNullOrEmpty(byte[] key) {
    return key == null || key.length == 0;
  }

  @Override
  public CassandraSubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < endpointFragmentMapping.size() : String.format("Mappings length [%d] should be greater than minor fragment id [%d] but it isn't.", endpointFragmentMapping.size(), minorFragmentId);
    return new CassandraSubScan(storagePlugin, storagePluginConfig, endpointFragmentMapping
      .get(minorFragmentId), columns);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return -1;
  }

  @Override
  public ScanStats getScanStats() {
    //TODO
    return ScanStats.TRIVIAL_TABLE;
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new CassandraGroupScan(this);
  }

  @JsonIgnore
  public CassandraStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  @JsonIgnore
  public String getTableName() {
    return getCassandraScanSpec().getTable();
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "CassandraGroupScan [CassandraScanSpec=" + cassandraScanSpec + ", columns=" + columns + "]";
  }

  @JsonProperty("storage")
  public CassandraStoragePluginConfig getStorageConfig() {
    return this.storagePluginConfig;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty
  public CassandraScanSpec getCassandraScanSpec() {
    return cassandraScanSpec;
  }

  @Override
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
  public CassandraGroupScan() {
    super((String) null);
  }

  /**
   * Do not use, only for testing.
   */
  @VisibleForTesting
  public void setCassandraScanSpec(CassandraScanSpec cassandraScanSpec) {
    this.cassandraScanSpec = cassandraScanSpec;
  }

}
