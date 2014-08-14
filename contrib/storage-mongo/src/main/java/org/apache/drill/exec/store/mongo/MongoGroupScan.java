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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
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
  
  @JsonIgnore
  private DBCollection collection;
  
  private Stopwatch watch = new Stopwatch();
  
  private boolean filterPushedDown = false;
  
  @JsonCreator
  public MongoGroupScan(@JsonProperty("mongoScanSpec") MongoScanSpec scanSpec,
                        @JsonProperty("storage") MongoStoragePluginConfig storagePluginConfig,
                        @JsonProperty("columns") List<SchemaPath> columns,
                        @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this ((MongoStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), scanSpec, columns);
  }
  
  public MongoGroupScan(MongoStoragePlugin storagePlugin, MongoScanSpec scanSpec, List<SchemaPath> columns) {
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.scanSpec = scanSpec;
    this.columns = columns;
    this.storagePluginConfig.getConnection();
    init();
  }
  
  /**
   * Private constructor, used for cloning.
   * @param that The MongoGroupScan to clone
   */
  private MongoGroupScan (MongoGroupScan that) {
    this.scanSpec = that.scanSpec;
    this.collection = that.collection;
    this.columns = that.columns;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.filterPushedDown = that.filterPushedDown;
  }
  
  private void init() {
    try {
      MongoClientURI clientURI = new MongoClientURI(this.storagePluginConfig.getConnection());
      MongoClient client = new MongoClient(clientURI);
      DB db = client.getDB(this.scanSpec.getDbName());
      collection = db.getCollection(this.scanSpec.getCollectionName());
    } catch (UnknownHostException e) {
      throw new DrillRuntimeException(e.getMessage(), e);
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
    
  }
  
  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
	MongoSubScanSpec subScanSpec = new MongoSubScanSpec(scanSpec.getDbName(), scanSpec.getCollectionName(), storagePluginConfig.getConnection(),scanSpec.getFilters());
	List<MongoSubScanSpec> subScanList = new LinkedList<MongoSubScanSpec>();
	subScanList.add(subScanSpec);
	return new MongoSubScan(this.storagePlugin, this.storagePluginConfig, subScanList, this.columns);
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
    if(collection != null){
      CommandResult stats = collection.getStats();
      return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, stats.getLong(COUNT), 1, (float) stats.getDouble(SIZE));
    }
    return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, 0, 1, 0);
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
  
  
  @JsonIgnore
  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  @JsonIgnore
  public void setFilterPushedDown(boolean filterPushedDown) {
    this.filterPushedDown = filterPushedDown;
  }

  @JsonIgnore
  public MongoStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  @Override
  public String toString() {
    return "MongoGroupScan [MongoScanSpec="
        + scanSpec + ", columns=" 
        + columns + "]";
  }

}
