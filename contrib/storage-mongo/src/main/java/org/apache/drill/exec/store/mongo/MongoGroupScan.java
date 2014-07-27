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
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;

import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

@JsonTypeName("mongo-scan")
public class MongoGroupScan extends AbstractGroupScan implements DrillMongoConstants {

  private static final String SIZE = "size";

  private static final String COUNT = "count";

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MongoGroupScan.class);
  
  private MongoStoragePlugin storagePlugin;
  
  private MongoStoragePluginConfig storagePluginConfig;
  
  private MongoScanSpec scanSpec;
  
  private List<SchemaPath> columns;
  
  private String url;
  
  private DBCollection collection;
  
  private Stopwatch watch = new Stopwatch();
  
  @JsonCreator
  public MongoGroupScan(@JsonProperty("url") String url,
                        @JsonProperty("storage") MongoStoragePluginConfig storagePluginConfig,
                        @JsonProperty("columns") List<SchemaPath> columns,
                        @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this ((MongoStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), url, columns);
  }
  
  public MongoGroupScan(MongoStoragePlugin storagePlugin, String url, List<SchemaPath> columns) {
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.url = url;
    this.columns = columns;
    this.storagePluginConfig.getConnection();
    init();
  }
  
  private void init() {
    try {
      MongoClientURI clientURI = new MongoClientURI(url);
      MongoClient client = new MongoClient(clientURI);
      DB db = client.getDB(clientURI.getDatabase());
      collection = db.getCollection(clientURI.getCollection());
    } catch (UnknownHostException e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    }
    
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    //Need to implement this.
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return new MongoSubScan(storagePlugin, storagePluginConfig, columns);
  }

  @Override
  public int getMaxParallelizationWidth() {
    // NO APIs to get the number of chunks, a collection has.
    return Integer.MAX_VALUE;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public ScanStats getScanStats() {
    CommandResult stats = collection.getStats();
    return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, stats.getLong(COUNT), 1, (float) stats.getDouble(SIZE));
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return null;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    watch.reset();
    watch.start();
    
    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<DrillbitEndpoint, EndpointAffinity>();
    
    //assign each end point equal affinity. No APIs to get shards information.
    for (DrillbitEndpoint ep : storagePlugin.getContext().getBits()) {
      affinityMap.put(ep, new EndpointAffinity(ep, 1));
      logger.debug("endpoing address: {}", ep.getAddress());
    }

    logger.debug("Took {} µs to get operator affinity", watch.elapsed(TimeUnit.NANOSECONDS)/1000);
    return Lists.newArrayList(affinityMap.values());
  }
  
  public DBCollection getCollection() {
    return collection;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }
  
  public MongoScanSpec getScanSpec() {
    return scanSpec;
  }

  @Override
  public String toString() {
    return "MongoGroupScan [MongoScanSpec="
        + scanSpec + ", columns=" 
        + columns + "]";
  }

}
