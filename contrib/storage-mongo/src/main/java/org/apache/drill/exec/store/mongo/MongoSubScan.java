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
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.mongodb.BasicDBObject;

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
  public MongoSubScan(@JacksonInject StoragePluginRegistry registry,
                      @JsonProperty("storagePlgConfig") StoragePluginConfig storagePluginConfig,
                      @JsonProperty("chunkScanSpecList") LinkedList<MongoSubScanSpec> chunkScanSpecList,
                      @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    this.columns = columns;
    this.mongoPluginConfig = (MongoStoragePluginConfig) storagePluginConfig;
    this.mongoStoragePlugin = (MongoStoragePlugin) registry.getPlugin(storagePluginConfig);
    this.chunkScanSpecList = chunkScanSpecList;
  }

  public MongoSubScan(MongoStoragePlugin storagePlugin,
                      MongoStoragePluginConfig storagePluginConfig,
                      List<MongoSubScanSpec> chunkScanSpecList,
                      List<SchemaPath> columns) {
    this.mongoStoragePlugin = storagePlugin;
    this.mongoPluginConfig = storagePluginConfig;
    this.columns = columns;
    this.chunkScanSpecList = chunkScanSpecList;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
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
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new MongoSubScan(mongoStoragePlugin, mongoPluginConfig, chunkScanSpecList, columns);
  }

  @Override
  public int getOperatorType() {
    return 1009;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class MongoSubScanSpec {
   
    protected String dbName;
    protected String collectionName;
    protected String connection;
    protected BasicDBObject filter;
    
    @parquet.org.codehaus.jackson.annotate.JsonCreator
    public MongoSubScanSpec(@JsonProperty("dbName") String dbName,
                            @JsonProperty("collectionName") String collectionName,
                            @JsonProperty("connection") String connection, @JsonProperty("filters") BasicDBObject filters) {
      this.dbName = dbName;
      this.collectionName = collectionName;
      this.connection = connection;
      this.filter = filters;
    }

    public String getConnection() {
      return connection;
    }
    
    public String getDbName() {
      return dbName;
    }

    public String getCollectionName() {
      return collectionName;
    }
    
    public BasicDBObject getFilter() {
      return filter;
    }

    @Override
    public String toString() {
      return "MongoSubScanSpec [tableName=" + collectionName
          + ", dbName=" + dbName
          + ", connection=" + connection + "]";
    }
  }

}
