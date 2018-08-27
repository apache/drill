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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

@JsonTypeName("mongo-shard-read")
public class MongoSubScan extends AbstractBase implements SubScan {
  static final Logger logger = LoggerFactory.getLogger(MongoSubScan.class);

  @JsonProperty
  private final MongoStoragePluginConfig mongoPluginConfig;
  @JsonIgnore
  private final MongoStoragePlugin mongoStoragePlugin;
  private final List<SchemaPath> columns;

  private final List<MongoSubScanSpec> chunkScanSpecList;

  @JsonCreator
  public MongoSubScan(
      @JacksonInject StoragePluginRegistry registry,
      @JsonProperty("userName") String userName,
      @JsonProperty("mongoPluginConfig") StoragePluginConfig mongoPluginConfig,
      @JsonProperty("chunkScanSpecList") LinkedList<MongoSubScanSpec> chunkScanSpecList,
      @JsonProperty("columns") List<SchemaPath> columns)
      throws ExecutionSetupException {
    super(userName);
    this.columns = columns;
    this.mongoPluginConfig = (MongoStoragePluginConfig) mongoPluginConfig;
    this.mongoStoragePlugin = (MongoStoragePlugin) registry
        .getPlugin(mongoPluginConfig);
    this.chunkScanSpecList = chunkScanSpecList;
  }

  public MongoSubScan(String userName, MongoStoragePlugin storagePlugin,
      MongoStoragePluginConfig storagePluginConfig,
      List<MongoSubScanSpec> chunkScanSpecList, List<SchemaPath> columns) {
    super(userName);
    this.mongoStoragePlugin = storagePlugin;
    this.mongoPluginConfig = storagePluginConfig;
    this.columns = columns;
    this.chunkScanSpecList = chunkScanSpecList;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @JsonIgnore
  public MongoStoragePluginConfig getMongoPluginConfig() {
    return mongoPluginConfig;
  }

  @JsonIgnore
  public MongoStoragePlugin getMongoStoragePlugin() {
    return mongoStoragePlugin;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public List<MongoSubScanSpec> getChunkScanSpecList() {
    return chunkScanSpecList;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new MongoSubScan(getUserName(), mongoStoragePlugin, mongoPluginConfig,
        chunkScanSpecList, columns);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.MONGO_SUB_SCAN_VALUE;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  public static class MongoSubScanSpec {

    protected String dbName;
    protected String collectionName;
    protected List<String> hosts;
    protected Map<String, Object> minFilters;
    protected Map<String, Object> maxFilters;

    protected Document filter;

    @JsonCreator
    public MongoSubScanSpec(@JsonProperty("dbName") String dbName,
        @JsonProperty("collectionName") String collectionName,
        @JsonProperty("hosts") List<String> hosts,
        @JsonProperty("minFilters") Map<String, Object> minFilters,
        @JsonProperty("maxFilters") Map<String, Object> maxFilters,
        @JsonProperty("filters") Document filters) {
      this.dbName = dbName;
      this.collectionName = collectionName;
      this.hosts = hosts;
      this.minFilters = minFilters;
      this.maxFilters = maxFilters;
      this.filter = filters;
    }

    MongoSubScanSpec() {

    }

    public String getDbName() {
      return dbName;
    }

    public MongoSubScanSpec setDbName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public String getCollectionName() {
      return collectionName;
    }

    public MongoSubScanSpec setCollectionName(String collectionName) {
      this.collectionName = collectionName;
      return this;
    }

    public List<String> getHosts() {
      return hosts;
    }

    public MongoSubScanSpec setHosts(List<String> hosts) {
      this.hosts = hosts;
      return this;
    }

    public Map<String, Object> getMinFilters() {
      return minFilters;
    }

    public MongoSubScanSpec setMinFilters(Map<String, Object> minFilters) {
      this.minFilters = minFilters;
      return this;
    }

    public Map<String, Object> getMaxFilters() {
      return maxFilters;
    }

    public MongoSubScanSpec setMaxFilters(Map<String, Object> maxFilters) {
      this.maxFilters = maxFilters;
      return this;
    }

    public Document getFilter() {
      return filter;
    }

    public MongoSubScanSpec setFilter(Document filter) {
      this.filter = filter;
      return this;
    }

    @Override
    public String toString() {
      return "MongoSubScanSpec [dbName=" + dbName + ", collectionName="
          + collectionName + ", hosts=" + hosts + ", minFilters=" + minFilters
          + ", maxFilters=" + maxFilters + ", filter=" + filter + "]";
    }

  }

}
