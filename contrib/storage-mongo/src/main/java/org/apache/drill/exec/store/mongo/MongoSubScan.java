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

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.bson.Document;
import org.bson.conversions.Bson;

@JsonTypeName("mongo-shard-read")
public class MongoSubScan extends AbstractBase implements SubScan {

  public static final String OPERATOR_TYPE = "MONGO_SUB_SCAN";

  @JsonProperty
  private final MongoStoragePluginConfig mongoPluginConfig;
  @JsonIgnore
  private final MongoStoragePlugin mongoStoragePlugin;
  private final List<SchemaPath> columns;

  private final List<BaseMongoSubScanSpec> chunkScanSpecList;

  @JsonCreator
  public MongoSubScan(
      @JacksonInject StoragePluginRegistry registry,
      @JsonProperty("userName") String userName,
      @JsonProperty("mongoPluginConfig") StoragePluginConfig mongoPluginConfig,
      @JsonProperty("chunkScanSpecList") LinkedList<BaseMongoSubScanSpec> chunkScanSpecList,
      @JsonProperty("columns") List<SchemaPath> columns)
      throws ExecutionSetupException {
    super(userName);
    this.columns = columns;
    this.mongoPluginConfig = (MongoStoragePluginConfig) mongoPluginConfig;
    this.mongoStoragePlugin = registry.resolve(
        mongoPluginConfig, MongoStoragePlugin.class);
    this.chunkScanSpecList = chunkScanSpecList;
  }

  public MongoSubScan(String userName, MongoStoragePlugin storagePlugin,
      MongoStoragePluginConfig storagePluginConfig,
      List<BaseMongoSubScanSpec> chunkScanSpecList, List<SchemaPath> columns) {
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

  public List<BaseMongoSubScanSpec> getChunkScanSpecList() {
    return chunkScanSpecList;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MongoSubScan(getUserName(), mongoStoragePlugin, mongoPluginConfig,
        chunkScanSpecList, columns);
  }

  @Override
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @JsonTypeName("ShardedMongoSubScanSpec")
  public static class ShardedMongoSubScanSpec extends BaseMongoSubScanSpec {

    protected Map<String, Object> minFilters;
    protected Map<String, Object> maxFilters;
    protected Document filter;

    @JsonCreator
    public ShardedMongoSubScanSpec(@JsonProperty("dbName") String dbName,
        @JsonProperty("collectionName") String collectionName,
        @JsonProperty("hosts") List<String> hosts,
        @JsonProperty("minFilters") Map<String, Object> minFilters,
        @JsonProperty("maxFilters") Map<String, Object> maxFilters,
        @JsonProperty("filters") Document filters) {
      super(dbName, collectionName, hosts);
      this.minFilters = minFilters;
      this.maxFilters = maxFilters;
      this.filter = filters;
    }

    ShardedMongoSubScanSpec() {
    }

    public Map<String, Object> getMinFilters() {
      return minFilters;
    }

    public ShardedMongoSubScanSpec setMinFilters(Map<String, Object> minFilters) {
      this.minFilters = minFilters;
      return this;
    }

    public Map<String, Object> getMaxFilters() {
      return maxFilters;
    }

    public ShardedMongoSubScanSpec setMaxFilters(Map<String, Object> maxFilters) {
      this.maxFilters = maxFilters;
      return this;
    }

    public Document getFilter() {
      return filter;
    }

    public ShardedMongoSubScanSpec setFilter(Document filter) {
      this.filter = filter;
      return this;
    }

    @Override
    public String toString() {
      return new PlanStringBuilder(this)
        .field("dbName", dbName)
        .field("collectionName", collectionName)
        .field("hosts", hosts)
        .field("minFilters", minFilters)
        .field("maxFilters", maxFilters)
        .field("filter", filter)
        .toString();
    }

  }

  @JsonTypeName("MongoSubScanSpec")
  public static class MongoSubScanSpec extends BaseMongoSubScanSpec {

    protected List<Bson> operations;

    @JsonCreator
    public MongoSubScanSpec(@JsonProperty("dbName") String dbName,
        @JsonProperty("collectionName") String collectionName,
        @JsonProperty("hosts") List<String> hosts,
        @JsonProperty("operations") List<Bson> operations) {
      super(dbName, collectionName, hosts);
      this.operations = operations;
    }

    MongoSubScanSpec() {
    }

    public List<Bson> getOperations() {
      return operations;
    }

    public MongoSubScanSpec setOperations(List<Bson> operations) {
      this.operations = operations;
      return this;
    }

    @Override
    public String toString() {
      return new PlanStringBuilder(this)
          .field("dbName", dbName)
          .field("collectionName", collectionName)
          .field("hosts", hosts)
          .field("operations", operations)
          .toString();
    }

  }

}
