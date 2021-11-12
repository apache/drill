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

import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
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
      @JsonProperty("columns") List<SchemaPath> columns) {
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

  @Getter
  @ToString
  @Jacksonized
  @SuperBuilder
  public static class ShardedMongoSubScanSpec extends BaseMongoSubScanSpec {

    @JsonProperty
    private final Map<String, Object> minFilters;

    @JsonProperty
    private final Map<String, Object> maxFilters;

    @JsonProperty
    private final String filter;

  }

  @Getter
  @ToString
  @Jacksonized
  @SuperBuilder
  public static class MongoSubScanSpec extends BaseMongoSubScanSpec {

    @JsonProperty
    private final List<String> operations;

  }

}
