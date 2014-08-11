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
package org.apache.drill.exec.store.mongo;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.mongo.MongoSubScan.MongoSubScanSpec;
import org.apache.drill.exec.store.mongo.common.ChunkInfo;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

@JsonTypeName("mongo-scan")
public class MongoGroupScan extends AbstractGroupScan implements DrillMongoConstants {

  private static final Integer select = Integer.valueOf(1);

  static final Logger logger = LoggerFactory.getLogger(MongoGroupScan.class);

  private MongoStoragePlugin storagePlugin;

  private MongoStoragePluginConfig storagePluginConfig;

  private MongoScanSpec scanSpec;

  private List<SchemaPath> columns;

  private Map<Integer, List<MongoSubScanSpec>> endpointFragmentMapping;

  // Sharding with replica sets contains all the replica server addresses for
  // each chunk.
  private Map<String, Set<ServerAddress>> chunksMapping;

  private Map<String, List<ChunkInfo>> chunksInverseMapping;

  private Stopwatch watch = new Stopwatch();

  @JsonCreator
  public MongoGroupScan(@JsonProperty("mongoScanSpec") MongoScanSpec scanSpec,
      @JsonProperty("storage") MongoStoragePluginConfig storagePluginConfig,
      @JsonProperty("columns") List<SchemaPath> columns, @JacksonInject StoragePluginRegistry pluginRegistry)
      throws IOException, ExecutionSetupException {
    this((MongoStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), scanSpec, columns);
  }

  public MongoGroupScan(MongoStoragePlugin storagePlugin, MongoScanSpec scanSpec, List<SchemaPath> columns) throws IOException {
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.scanSpec = scanSpec;
    this.columns = columns;
    this.storagePluginConfig.getConnection();
    init();
  }

  /**
   * Private constructor, used for cloning.
   * 
   * @param that
   *          The MongoGroupScan to clone
   */
  private MongoGroupScan(MongoGroupScan that) {
    this.scanSpec = that.scanSpec;
    this.columns = that.columns;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.chunksMapping = that.chunksMapping;
    this.chunksInverseMapping = that.chunksInverseMapping;
    this.endpointFragmentMapping = that.endpointFragmentMapping;
  }

  @SuppressWarnings("rawtypes")
  private void init() throws IOException {
    MongoClient client = null;
    try {
      MongoClientURI clientURI = new MongoClientURI(this.storagePluginConfig.getConnection());
      client = new MongoClient(clientURI);
      List<String> databaseNames = client.getDatabaseNames();

      chunksMapping = Maps.newHashMap();
      chunksInverseMapping = Maps.newHashMap();
      if (databaseNames.contains(CONFIG)) {
        DB db = client.getDB(CONFIG);
        db.setReadPreference(ReadPreference.nearest());
        DBCollection chunksCollection = db.getCollectionFromString(CHUNKS);

        DBObject query = new BasicDBObject(1);
        query.put(NS, this.scanSpec.getDbName() + "." + this.scanSpec.getCollectionName());

        DBObject fields = new BasicDBObject();
        fields.put(SHARD, select);
        fields.put(MIN, select);
        fields.put(MAX, select);

        DBCursor chunkCursor = chunksCollection.find(query, fields);

        DBCollection shardsCollection = db.getCollectionFromString(SHARDS);

        fields = new BasicDBObject();
        fields.put(HOST, select);

        while (chunkCursor.hasNext()) {
          DBObject chunkObj = chunkCursor.next();
          String shardName = (String) chunkObj.get(SHARD);
          String chunkId = (String) chunkObj.get(ID);
          query = new BasicDBObject().append(ID, shardName);
          DBCursor hostCursor = shardsCollection.find(query, fields);
          while (hostCursor.hasNext()) {
            DBObject hostObj = hostCursor.next();
            String host = (String) hostObj.get(HOST); //needs to verify this
            String[] tagAndHost = StringUtils.split(host, '/');
            if (tagAndHost.length > 1) {
              host = tagAndHost[1];
            }
            Set<ServerAddress> addressList = chunksMapping.get(chunkId);
            if (addressList == null) {
              addressList = Sets.newHashSet();
              chunksMapping.put(chunkId, addressList);
            }
            ServerAddress address = new ServerAddress(host);
            addressList.add(address);
            
            List<ChunkInfo> chunkList = chunksInverseMapping.get(address.getHost());
            if (chunkList == null) {
              chunkList = Lists.newArrayList();
              chunksInverseMapping.put(address.getHost(), chunkList);
            }
            List<String> hosts = new ArrayList<>();
            hosts.add(host);
            ChunkInfo chunkInfo = new ChunkInfo(hosts, chunkId);
            DBObject minObj = (BasicDBObject) chunkObj.get(MIN);
            
            Map<String, Object> minFilters = Maps.newHashMap();
            Map minMap = minObj.toMap();
            Set keySet = minMap.keySet();
            for(Object keyObj : keySet) {
              Object object = minMap.get(keyObj);
              if(!(object instanceof MinKey) && object instanceof Serializable) {
                minFilters.put(keyObj.toString(), object);
              }
            }
            chunkInfo.setMinFilters(minFilters);
            
            DBObject maxObj = (BasicDBObject) chunkObj.get(MAX);
            Map<String, Object> maxFilters = Maps.newHashMap();
            Map maxMap = maxObj.toMap();
            keySet = maxMap.keySet();
            for(Object keyObj : keySet) {
              Object object = maxMap.get(keyObj);
              if(!(object instanceof MaxKey) && object instanceof Serializable) {
                maxFilters.put(keyObj.toString(), object);
              }
            }
            
            chunkInfo.setMaxFilters(maxFilters);
            chunkList.add(chunkInfo);
          }
        }
      } else {
        String chunkName = scanSpec.getDbName() + "." + scanSpec.getCollectionName();
        MongoClientURI uri = new MongoClientURI(this.storagePluginConfig.getConnection());
        List<String> hosts = uri.getHosts();
        Set<ServerAddress> addressList = Sets.newHashSet();
        
        for (String host : hosts) {
          addressList.add(new ServerAddress(host));
        }
        chunksMapping.put(chunkName, addressList);
        
        String host = hosts.get(0);
        ServerAddress address = new ServerAddress(host);
        ChunkInfo chunkInfo = new ChunkInfo(hosts, chunkName);
        List<ChunkInfo> chunksList = Lists.newArrayList();
        chunksList.add(chunkInfo);
        chunksInverseMapping.put(address.getHost(), chunksList);
      }
      client.close();
    } catch (UnknownHostException e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    } finally {
      client.close();
    }

  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    MongoGroupScan clone = new MongoGroupScan(this);
    clone.columns = columns;
    return clone;
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    endpointFragmentMapping = Maps.newHashMap();
    // As of now, performing direct assignments to drill bits
    int fragmentIndex = 0;
    for (int fragmentId = 0; fragmentId < endpoints.size(); ++fragmentId) {
      DrillbitEndpoint ep = endpoints.get(fragmentId);
      List<MongoSubScanSpec> mongoSubScanSpecList = Lists.newArrayList();
      List<ChunkInfo> chunks = chunksInverseMapping.get(ep.getAddress());
      if (chunks == null || chunks.isEmpty()) {
        continue;
      }
      for (int i = 0; i < chunks.size(); ++i) {
        ChunkInfo chunkInfo = chunks.get(i);
        MongoSubScanSpec spec = new MongoSubScanSpec().setDbName(scanSpec.getDbName())
            .setCollectionName(scanSpec.getCollectionName()).setHosts(chunkInfo.getChunkLocList());
        mongoSubScanSpecList.add(spec);
      }
      endpointFragmentMapping.put(Integer.valueOf(fragmentIndex++), mongoSubScanSpecList);
    }
    
    //If drill bits and mongo servers are not running on same machines, assign in round robin fashion.
    if (endpointFragmentMapping.isEmpty()) {
      int endPointsSize = endpoints.size();
      Collection<List<ChunkInfo>> values = chunksInverseMapping.values();
      Iterator<List<ChunkInfo>> iterator = values.iterator();
      for (fragmentIndex = 0; fragmentIndex < values.size(); ++fragmentIndex) {
        List<MongoSubScanSpec> list = endpointFragmentMapping.get(fragmentIndex % endPointsSize);
        if (list == null) {
          list = Lists.newArrayList();
          endpointFragmentMapping.put(fragmentIndex % endPointsSize, list);
        }
        // Take the first replica info, for assignment
        List<ChunkInfo> chunksList = iterator.next();
        for(ChunkInfo chunkInfo : chunksList) {
          MongoSubScanSpec spec = new MongoSubScanSpec().setDbName(scanSpec.getDbName())
              .setCollectionName(scanSpec.getCollectionName()).setHosts(chunkInfo.getChunkLocList())
              .setMinFilters(chunkInfo.getMinFilters()).setMaxFilters(chunkInfo.getMaxFilters());
          list.add(spec);
        }
      }
    }
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return new MongoSubScan(storagePlugin, storagePluginConfig, endpointFragmentMapping.get(minorFragmentId), columns);
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
    MongoClientURI clientURI = new MongoClientURI(this.storagePluginConfig.getConnection());
    MongoClient client = null;
    try {
      client = new MongoClient(clientURI);
      DB db = client.getDB(scanSpec.getDbName());
      db.setReadPreference(ReadPreference.nearest());
      DBCollection collection = db.getCollectionFromString(scanSpec.getCollectionName());
      CommandResult stats = collection.getStats();
      return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, stats.getLong(COUNT), 1, (float) stats.getDouble(SIZE));
    } catch (Exception e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
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
      //Each replica can be on multiple machines, take the first one, which meets affinity.
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
    logger.debug("Took {} µs to get operator affinity", watch.elapsed(TimeUnit.NANOSECONDS) / 1000);
    return Lists.newArrayList(affinityMap.values());
  }

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

  @Override
  public String toString() {
    return "MongoGroupScan [MongoScanSpec=" + scanSpec + ", columns=" + columns + "]";
  }

}
