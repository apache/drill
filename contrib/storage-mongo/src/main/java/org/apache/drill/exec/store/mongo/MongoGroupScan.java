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
package org.apache.drill.exec.store.mongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.mongodb.client.MongoClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.mongo.MongoSubScan.MongoSubScanSpec;
import org.apache.drill.exec.store.mongo.common.ChunkInfo;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.drill.shaded.guava.com.google.common.base.Joiner;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

@JsonTypeName("mongo-scan")
public class MongoGroupScan extends AbstractGroupScan implements
    DrillMongoConstants {

  private static final Integer select = 1;

  private static final Logger logger = LoggerFactory.getLogger(MongoGroupScan.class);

  private static final Comparator<List<MongoSubScanSpec>> LIST_SIZE_COMPARATOR = Comparator.comparingInt(List::size);

  private static final Comparator<List<MongoSubScanSpec>> LIST_SIZE_COMPARATOR_REV = Collections.reverseOrder(LIST_SIZE_COMPARATOR);

  private MongoStoragePlugin storagePlugin;

  private MongoStoragePluginConfig storagePluginConfig;

  private MongoScanSpec scanSpec;

  private List<SchemaPath> columns;

  private int maxRecords;

  private Map<Integer, List<MongoSubScanSpec>> endpointFragmentMapping;

  // Sharding with replica sets contains all the replica server addresses for
  // each chunk.
  private Map<String, Set<ServerAddress>> chunksMapping;

  private Map<String, List<ChunkInfo>> chunksInverseMapping;

  private final Stopwatch watch = Stopwatch.createUnstarted();

  private boolean filterPushedDown = false;

  @JsonCreator
  public MongoGroupScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("mongoScanSpec") MongoScanSpec scanSpec,
      @JsonProperty("storage") MongoStoragePluginConfig storagePluginConfig,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JacksonInject StoragePluginRegistry pluginRegistry,
      @JsonProperty("maxRecords") int maxRecords) throws IOException {
    this(userName,
        pluginRegistry.resolve(storagePluginConfig, MongoStoragePlugin.class),
        scanSpec, columns, maxRecords);
  }

  public MongoGroupScan(String userName, MongoStoragePlugin storagePlugin,
      MongoScanSpec scanSpec, List<SchemaPath> columns, int maxRecords) throws IOException {
    super(userName);
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.scanSpec = scanSpec;
    this.columns = columns;
    this.maxRecords = maxRecords;
    init();
  }


  /**
   * Private constructor, used for cloning.
   * @param that
   *          The MongoGroupScan to clone
   */
  private MongoGroupScan(MongoGroupScan that) {
    super(that);
    this.scanSpec = that.scanSpec;
    this.columns = that.columns;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.chunksMapping = that.chunksMapping;
    this.chunksInverseMapping = that.chunksInverseMapping;
    this.endpointFragmentMapping = that.endpointFragmentMapping;
    this.filterPushedDown = that.filterPushedDown;
    this.maxRecords = that.maxRecords;
  }

  @JsonIgnore
  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  @JsonIgnore
  public void setFilterPushedDown(boolean filterPushedDown) {
    this.filterPushedDown = filterPushedDown;
  }

  private boolean isShardedCluster(MongoClient client) {
    MongoDatabase db = client.getDatabase(scanSpec.getDbName());
    String msg = db.runCommand(new Document("isMaster", 1)).getString("msg");
    return msg != null && msg.equals("isdbgrid");
  }

  @SuppressWarnings({ "rawtypes" })
  private void init() {

    List<String> h = storagePluginConfig.getHosts();
    List<ServerAddress> addresses = Lists.newArrayList();
    for (String host : h) {
      addresses.add(new ServerAddress(host));
    }
    MongoClient client = storagePlugin.getClient();
    chunksMapping = Maps.newHashMap();
    chunksInverseMapping = Maps.newLinkedHashMap();
    if (isShardedCluster(client)) {
      MongoDatabase db = client.getDatabase(CONFIG);
      MongoCollection<Document> chunksCollection = db.getCollection(CHUNKS);
      Document filter = new Document();
      filter
          .put(
              NS,
              this.scanSpec.getDbName() + "."
                  + this.scanSpec.getCollectionName());

      Document projection = new Document();
      projection.put(SHARD, select);
      projection.put(MIN, select);
      projection.put(MAX, select);

      FindIterable<Document> chunkCursor = chunksCollection.find(filter).projection(projection);
      MongoCursor<Document> iterator = chunkCursor.iterator();

      MongoCollection<Document> shardsCollection = db.getCollection(SHARDS);

      projection = new Document();
      projection.put(HOST, select);

      boolean hasChunks = false;
      while (iterator.hasNext()) {
        Document chunkObj = iterator.next();
        String shardName = (String) chunkObj.get(SHARD);
        // creates hexadecimal string representation of ObjectId
        String chunkId = chunkObj.get(ID).toString();
        filter = new Document(ID, shardName);
        FindIterable<Document> hostCursor = shardsCollection.find(filter).projection(projection);
        for (Document hostObj : hostCursor) {
          String hostEntry = (String) hostObj.get(HOST);
          String[] tagAndHost = StringUtils.split(hostEntry, '/');
          String[] hosts = tagAndHost.length > 1 ? StringUtils.split(
              tagAndHost[1], ',') : StringUtils.split(tagAndHost[0], ',');
          Set<ServerAddress> addressList = getPreferredHosts(storagePlugin.getClient(addresses));
          if (addressList == null) {
            addressList = Sets.newHashSet();
            for (String host : hosts) {
              addressList.add(new ServerAddress(host));
            }
          }
          chunksMapping.put(chunkId, addressList);
          ServerAddress address = addressList.iterator().next();
          List<ChunkInfo> chunkList = chunksInverseMapping.computeIfAbsent(address.getHost(), k -> new ArrayList<>());
          List<String> chunkHostsList = new ArrayList<>();
          for (ServerAddress serverAddr : addressList) {
            chunkHostsList.add(serverAddr.toString());
          }
          ChunkInfo chunkInfo = new ChunkInfo(chunkHostsList, chunkId);
          Document minMap = (Document) chunkObj.get(MIN);

          Map<String, Object> minFilters = Maps.newHashMap();
          Set keySet = minMap.keySet();
          for (Object keyObj : keySet) {
            Object object = minMap.get(keyObj);
            if (!(object instanceof MinKey)) {
              minFilters.put(keyObj.toString(), object);
            }
          }
          chunkInfo.setMinFilters(minFilters);

          Map<String, Object> maxFilters = Maps.newHashMap();
          Map maxMap = (Document) chunkObj.get(MAX);
          keySet = maxMap.keySet();
          for (Object keyObj : keySet) {
            Object object = maxMap.get(keyObj);
            if (!(object instanceof MaxKey)) {
              maxFilters.put(keyObj.toString(), object);
            }
          }

          chunkInfo.setMaxFilters(maxFilters);
          chunkList.add(chunkInfo);
        }
        hasChunks = true;
      }
      // In a sharded environment, if a collection doesn't have any chunks, it is considered as an
      // unsharded collection and it will be stored in the primary shard of that database.
      if (!hasChunks) {
        handleUnshardedCollection(getPrimaryShardInfo());
      }
    } else {
      handleUnshardedCollection(storagePluginConfig.getHosts());
    }

  }

  private void handleUnshardedCollection(List<String> hosts) {
    String chunkName = Joiner.on('.').join(scanSpec.getDbName(), scanSpec.getCollectionName());
    Set<ServerAddress> addressList = Sets.newHashSet();

    for (String host : hosts) {
      addressList.add(new ServerAddress(host));
    }
    chunksMapping.put(chunkName, addressList);

    String host = hosts.get(0);
    ServerAddress address = new ServerAddress(host);
    ChunkInfo chunkInfo = new ChunkInfo(hosts, chunkName);
    chunkInfo.setMinFilters(Collections.emptyMap());
    chunkInfo.setMaxFilters(Collections.emptyMap());
    List<ChunkInfo> chunksList = Lists.newArrayList();
    chunksList.add(chunkInfo);
    chunksInverseMapping.put(address.getHost(), chunksList);
  }

  private List<String> getPrimaryShardInfo() {
    MongoDatabase database = storagePlugin.getClient().getDatabase(CONFIG);
    //Identify the primary shard of the queried database.
    MongoCollection<Document> collection = database.getCollection(DATABASES);
    Bson filter = new Document(ID, this.scanSpec.getDbName());
    Bson projection = new Document(PRIMARY, select);
    Document document = Objects.requireNonNull(collection.find(filter).projection(projection).first());
    String shardName = document.getString(PRIMARY);
    Preconditions.checkNotNull(shardName);

    //Identify the host(s) on which this shard resides.
    MongoCollection<Document> shardsCol = database.getCollection(SHARDS);
    filter = new Document(ID, shardName);
    projection = new Document(HOST, select);
    Document hostInfo = Objects.requireNonNull(shardsCol.find(filter).projection(projection).first());
    String hostEntry = hostInfo.getString(HOST);
    Preconditions.checkNotNull(hostEntry);

    String[] tagAndHost = StringUtils.split(hostEntry, '/');
    String[] hosts = tagAndHost.length > 1 ? StringUtils.split(tagAndHost[1],
        ',') : StringUtils.split(tagAndHost[0], ',');
    return Lists.newArrayList(hosts);
  }

  @SuppressWarnings("unchecked")
  private Set<ServerAddress> getPreferredHosts(MongoClient client) {
    Set<ServerAddress> addressList = Sets.newHashSet();
    MongoDatabase db = client.getDatabase(scanSpec.getDbName());
    ReadPreference readPreference = db.getReadPreference();
    Document command = db.runCommand(new Document("isMaster", 1));

    final String primaryHost = command.getString("primary");
    final List<String> hostsList = (List<String>) command.get("hosts");

    switch (readPreference.getName().toUpperCase()) {
    case "PRIMARY":
    case "PRIMARYPREFERRED":
      if (primaryHost == null) {
        return null;
      }
      addressList.add(new ServerAddress(primaryHost));
      return addressList;
    case "SECONDARY":
    case "SECONDARYPREFERRED":
      if (primaryHost == null || hostsList == null) {
        return null;
      }
      hostsList.remove(primaryHost);
      for (String host : hostsList) {
        addressList.add(new ServerAddress(host));
      }
      return addressList;
    case "NEAREST":
      if (hostsList == null) {
        return null;
      }
      for (String host : hostsList) {
        addressList.add(new ServerAddress(host));
      }
      return addressList;
    default:
      return null;
    }
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    MongoGroupScan clone = new MongoGroupScan(this);
    clone.columns = columns;
    return clone;
  }

  public GroupScan clone(int maxRecords) {
    MongoGroupScan clone = new MongoGroupScan(this);
    clone.maxRecords = maxRecords;
    return clone;
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    logger.debug("Incoming endpoints :" + endpoints);
    watch.reset();
    watch.start();

    final int numSlots = endpoints.size();
    int totalAssignmentsTobeDone = chunksMapping.size();

    Preconditions.checkArgument(numSlots <= totalAssignmentsTobeDone, String
        .format("Incoming endpoints %d is greater than number of chunks %d",
            numSlots, totalAssignmentsTobeDone));

    final int minPerEndpointSlot = (int) Math
        .floor((double) totalAssignmentsTobeDone / numSlots);
    final int maxPerEndpointSlot = (int) Math
        .ceil((double) totalAssignmentsTobeDone / numSlots);

    endpointFragmentMapping = Maps.newHashMapWithExpectedSize(numSlots);
    Map<String, Queue<Integer>> endpointHostIndexListMap = Maps.newHashMap();

    for (int i = 0; i < numSlots; ++i) {
      endpointFragmentMapping.put(i, new ArrayList<>(
          maxPerEndpointSlot));
      String hostname = endpoints.get(i).getAddress();
      Queue<Integer> hostIndexQueue = endpointHostIndexListMap.computeIfAbsent(hostname, k -> new LinkedList<>());
      hostIndexQueue.add(i);
    }

    Set<Entry<String, List<ChunkInfo>>> chunksToAssignSet = Sets
        .newHashSet(chunksInverseMapping.entrySet());

    for (Iterator<Entry<String, List<ChunkInfo>>> chunksIterator = chunksToAssignSet
        .iterator(); chunksIterator.hasNext();) {
      Entry<String, List<ChunkInfo>> chunkEntry = chunksIterator.next();
      Queue<Integer> slots = endpointHostIndexListMap.get(chunkEntry.getKey());
      if (slots != null) {
        for (ChunkInfo chunkInfo : chunkEntry.getValue()) {
          Integer slotIndex = slots.poll();
          List<MongoSubScanSpec> subScanSpecList = endpointFragmentMapping
              .get(slotIndex);
          subScanSpecList.add(buildSubScanSpecAndGet(chunkInfo));
          slots.offer(slotIndex);
        }
        chunksIterator.remove();
      }
    }

    PriorityQueue<List<MongoSubScanSpec>> minHeap = new PriorityQueue<>(
        numSlots, LIST_SIZE_COMPARATOR);
    PriorityQueue<List<MongoSubScanSpec>> maxHeap = new PriorityQueue<>(
        numSlots, LIST_SIZE_COMPARATOR_REV);
    for (List<MongoSubScanSpec> listOfScan : endpointFragmentMapping.values()) {
      if (listOfScan.size() < minPerEndpointSlot) {
        minHeap.offer(listOfScan);
      } else if (listOfScan.size() > minPerEndpointSlot) {
        maxHeap.offer(listOfScan);
      }
    }

    if (chunksToAssignSet.size() > 0) {
      for (Entry<String, List<ChunkInfo>> chunkEntry : chunksToAssignSet) {
        for (ChunkInfo chunkInfo : chunkEntry.getValue()) {
          List<MongoSubScanSpec> smallestList = minHeap.poll();
          smallestList.add(buildSubScanSpecAndGet(chunkInfo));
          minHeap.offer(smallestList);
        }
      }
    }

    while (minHeap.peek() != null && minHeap.peek().size() < minPerEndpointSlot) {
      List<MongoSubScanSpec> smallestList = minHeap.poll();
      List<MongoSubScanSpec> largestList = maxHeap.poll();
      smallestList.add(largestList.remove(largestList.size() - 1));
      if (largestList.size() > minPerEndpointSlot) {
        maxHeap.offer(largestList);
      }
      if (smallestList.size() < minPerEndpointSlot) {
        minHeap.offer(smallestList);
      }
    }

    logger.debug(
        "Built assignment map in {} µs.\nEndpoints: {}.\nAssignment Map: {}",
        watch.elapsed(TimeUnit.NANOSECONDS) / 1000, endpoints,
        endpointFragmentMapping.toString());
  }

  private MongoSubScanSpec buildSubScanSpecAndGet(ChunkInfo chunkInfo) {
    return new MongoSubScanSpec()
        .setDbName(scanSpec.getDbName())
        .setCollectionName(scanSpec.getCollectionName())
        .setHosts(chunkInfo.getChunkLocList())
        .setMinFilters(chunkInfo.getMinFilters())
        .setMaxFilters(chunkInfo.getMaxFilters())
        .setMaxRecords(maxRecords)
        .setFilter(scanSpec.getFilters());
  }

  @Override
  public MongoSubScan getSpecificScan(int minorFragmentId) {
    return new MongoSubScan(getUserName(), storagePlugin, storagePluginConfig,
        endpointFragmentMapping.get(minorFragmentId), columns);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return chunksMapping.size();
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public ScanStats getScanStats() {
    long recordCount;
    try{
      MongoClient client = storagePlugin.getClient();
      MongoDatabase db = client.getDatabase(scanSpec.getDbName());
      MongoCollection<Document> collection = db.getCollection(scanSpec.getCollectionName());
      long numDocs = collection.estimatedDocumentCount();

      if (maxRecords > 0 && numDocs > 0) {
        recordCount = Math.min(maxRecords, numDocs);
      } else {
        recordCount = numDocs;
      }

      float approxDiskCost = 0;
      if (recordCount != 0) {
        //toJson should use client's codec, otherwise toJson could fail on
        // some types not known to DocumentCodec, e.g. DBRef.
        DocumentCodec codec = new DocumentCodec(db.getCodecRegistry(), new BsonTypeClassMap());
        String json = collection.find().first().toJson(codec);
        approxDiskCost = json.getBytes().length * recordCount;
      }
      return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, recordCount, 1, approxDiskCost);
    } catch (Exception e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MongoGroupScan(this);
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    watch.reset();
    watch.start();

    Map<String, DrillbitEndpoint> endpointMap = Maps.newHashMap();
    for (DrillbitEndpoint endpoint : storagePlugin.getContext().getBits()) {
      endpointMap.put(endpoint.getAddress(), endpoint);
      logger.debug("Endpoint address: {}", endpoint.getAddress());
    }

    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = Maps.newHashMap();
    // As of now, considering only the first replica, though there may be
    // multiple replicas for each chunk.
    for (Set<ServerAddress> addressList : chunksMapping.values()) {
      // Each replica can be on multiple machines, take the first one, which
      // meets affinity.
      for (ServerAddress address : addressList) {
        DrillbitEndpoint ep = endpointMap.get(address.getHost());
        if (ep != null) {
          EndpointAffinity affinity = affinityMap.get(ep);
          if (affinity == null) {
            affinityMap.put(ep, new EndpointAffinity(ep, 1));
          } else {
            affinity.addAffinity(1);
          }
          break;
        }
      }
    }
    logger.debug("Took {} µs to get operator affinity",
        watch.elapsed(TimeUnit.NANOSECONDS) / 1000);
    logger.debug("Affined drillbits : " + affinityMap.values());
    return Lists.newArrayList(affinityMap.values());
  }

  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    if (maxRecords == this.maxRecords) {
      return null;
    }
    return clone(maxRecords);
  }

  @Override
  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("mongoScanSpec")
  public MongoScanSpec getScanSpec() {
    return scanSpec;
  }

  @JsonProperty("storage")
  public MongoStoragePluginConfig getStorageConfig() {
    return storagePluginConfig;
  }

  @JsonProperty("maxRecords")
  public int getMaxRecords() { return maxRecords; }

  @JsonIgnore
  public MongoStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("MongoScanSpec", scanSpec)
      .field("columns", columns)
      .field("maxRecords", maxRecords)
      .toString();
  }

  @VisibleForTesting
  MongoGroupScan() {
    super((String)null);
  }

  @JsonIgnore
  @VisibleForTesting
  void setChunksMapping(Map<String, Set<ServerAddress>> chunksMapping) {
    this.chunksMapping = chunksMapping;
  }

  @JsonIgnore
  @VisibleForTesting
  void setScanSpec(MongoScanSpec scanSpec) {
    this.scanSpec = scanSpec;
  }

  @JsonIgnore
  @VisibleForTesting
  void setInverseChunsMapping(Map<String, List<ChunkInfo>> chunksInverseMapping) {
    this.chunksInverseMapping = chunksInverseMapping;
  }
}
