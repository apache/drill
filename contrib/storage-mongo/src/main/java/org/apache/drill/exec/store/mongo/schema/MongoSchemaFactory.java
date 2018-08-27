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
package org.apache.drill.exec.store.mongo.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.mongo.MongoScanSpec;
import org.apache.drill.exec.store.mongo.MongoStoragePlugin;
import org.apache.drill.exec.store.mongo.MongoStoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;

public class MongoSchemaFactory extends AbstractSchemaFactory {

  private static final Logger logger = LoggerFactory.getLogger(MongoSchemaFactory.class);

  private static final String DATABASES = "databases";

  private LoadingCache<String, List<String>> databases;
  private LoadingCache<String, List<String>> tableNameLoader;
  private final MongoStoragePlugin plugin;

  public MongoSchemaFactory(MongoStoragePlugin plugin, String schemaName) throws ExecutionSetupException {
    super(schemaName);
    this.plugin = plugin;

    databases = CacheBuilder //
        .newBuilder() //
        .expireAfterAccess(1, TimeUnit.MINUTES) //
        .build(new DatabaseLoader());

    tableNameLoader = CacheBuilder //
        .newBuilder() //
        .expireAfterAccess(1, TimeUnit.MINUTES) //
        .build(new TableNameLoader());
  }

  private class DatabaseLoader extends CacheLoader<String, List<String>> {

    @Override
    public List<String> load(String key) throws Exception {
      if (!DATABASES.equals(key)) {
        throw new UnsupportedOperationException();
      }
      try {
        List<String> dbNames = new ArrayList<>();
        plugin.getClient().listDatabaseNames().into(dbNames);
        return dbNames;
      } catch (MongoException me) {
        logger.warn("Failure while loading databases in Mongo. {}",
            me.getMessage());
        return Collections.emptyList();
      } catch (Exception e) {
        throw new DrillRuntimeException(e.getMessage(), e);
      }
    }

  }

  private class TableNameLoader extends CacheLoader<String, List<String>> {

    @Override
    public List<String> load(String dbName) throws Exception {
      try {
        MongoDatabase db = plugin.getClient().getDatabase(dbName);
        List<String> collectionNames = new ArrayList<>();
        db.listCollectionNames().into(collectionNames);
        return collectionNames;
      } catch (MongoException me) {
        logger.warn("Failure while getting collection names from '{}'. {}",
            dbName, me.getMessage());
        return Collections.emptyList();
      } catch (Exception e) {
        throw new DrillRuntimeException(e.getMessage(), e);
      }
    }
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    MongoSchema schema = new MongoSchema(getName());
    SchemaPlus hPlus = parent.add(getName(), schema);
    schema.setHolder(hPlus);
  }

  class MongoSchema extends AbstractSchema {

    private final Map<String, MongoDatabaseSchema> schemaMap = Maps.newHashMap();

    public MongoSchema(String name) {
      super(ImmutableList.<String> of(), name);
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      List<String> tables;
      try {
        if (! schemaMap.containsKey(name)) {
          tables = tableNameLoader.get(name);
          schemaMap.put(name, new MongoDatabaseSchema(tables, this, name));
        }

        return schemaMap.get(name);

        //return new MongoDatabaseSchema(tables, this, name);
      } catch (ExecutionException e) {
        logger.warn("Failure while attempting to access MongoDataBase '{}'.",
            name, e.getCause());
        return null;
      }

    }

    void setHolder(SchemaPlus plusOfThis) {
      for (String s : getSubSchemaNames()) {
        plusOfThis.add(s, getSubSchema(s));
      }
    }

    @Override
    public boolean showInInformationSchema() {
      return false;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      try {
        List<String> dbs = databases.get(DATABASES);
        return Sets.newHashSet(dbs);
      } catch (ExecutionException e) {
        logger.warn("Failure while getting Mongo database list.", e);
        return Collections.emptySet();
      }
    }

    List<String> getTableNames(String dbName) {
      try {
        return tableNameLoader.get(dbName);
      } catch (ExecutionException e) {
        logger.warn("Failure while loading table names for database '{}'.",
            dbName, e.getCause());
        return Collections.emptyList();
      }
    }

    DrillTable getDrillTable(String dbName, String collectionName) {
      MongoScanSpec mongoScanSpec = new MongoScanSpec(dbName, collectionName);
      return new DynamicDrillTable(plugin, getName(), null, mongoScanSpec);
    }

    @Override
    public String getTypeName() {
      return MongoStoragePluginConfig.NAME;
    }
  }
}
