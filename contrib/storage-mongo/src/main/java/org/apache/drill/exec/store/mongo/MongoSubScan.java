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

import java.util.Iterator;
import java.util.List;

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
import com.google.common.base.Preconditions;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

@JsonTypeName("mongo-shard-read")
public class MongoSubScan extends AbstractBase implements SubScan {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MongoSubScan.class);

  @JsonProperty
  private final MongoStoragePluginConfig mongoPluginConfig;
  @JsonIgnore
  private final MongoStoragePlugin mongoStoragePlugin;
  private final List<SchemaPath> columnSchemas;
  private final List<MongoSubScanSpec> shardList;

  @JsonCreator
  public MongoSubScan(
      @JacksonInject StoragePluginRegistry registry,
      @JsonProperty("storagePlgConfig") StoragePluginConfig storagePluginConfig,
      @JsonProperty("shardLists") List<MongoSubScanSpec> shardLists,
      @JsonProperty("columnsSchemas") List<SchemaPath> columns)
      throws ExecutionSetupException {
    this.columnSchemas = columns;
    this.shardList = shardLists;
    this.mongoPluginConfig = (MongoStoragePluginConfig) storagePluginConfig;
    this.mongoStoragePlugin = (MongoStoragePlugin) registry
        .getPlugin(storagePluginConfig);
  }

  public MongoSubScan(MongoStoragePlugin storagePlugin,
      MongoStoragePluginConfig storagePluginConfig,
      List<MongoSubScanSpec> shardLists, List<SchemaPath> columnPath) {
    this.mongoStoragePlugin = storagePlugin;
    this.mongoPluginConfig = storagePluginConfig;
    this.columnSchemas = columnPath;
    this.shardList = shardLists;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  public MongoStoragePluginConfig getMongoPluginConfig() {
    return mongoPluginConfig;
  }

  public MongoStoragePlugin getMongoStoragePlugin() {
    return mongoStoragePlugin;
  }

  public List<SchemaPath> getColumnSchemas() {
    return columnSchemas;
  }

  public List<MongoSubScanSpec> getShardList() {
    return shardList;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new MongoSubScan(mongoStoragePlugin, mongoPluginConfig, shardList,
        columnSchemas);
  }

  @Override
  public int getOperatorType() {
    return 0;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return null;
  }

  public static class MongoSubScanSpec {
    protected MongoClient mongoClient;
    protected String dbName;
    protected String collectionName;
    protected WriteConcern writeConcern;

    public MongoClient getMongoClient() {
      return mongoClient;
    }

    public void setMongoClient(MongoClient mongoClient) {
      this.mongoClient = mongoClient;
    }

    public String getDbName() {
      return dbName;
    }

    public void setDbName(String dbName) {
      this.dbName = dbName;
    }

    public String getCollectionName() {
      return collectionName;
    }

    public void setCollectionName(String collectionName) {
      this.collectionName = collectionName;
    }

    public WriteConcern getWriteConcern() {
      return writeConcern;
    }

    public void setWriteConcern(WriteConcern writeConcern) {
      this.writeConcern = writeConcern;
    }
  }

}
