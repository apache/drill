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
package org.apache.drill.exec.store.mongo.schema;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.mongo.MongoCnxnManager;
import org.apache.drill.exec.store.mongo.MongoScanSpec;
import org.apache.drill.exec.store.mongo.MongoStoragePlugin;
import org.apache.drill.exec.store.mongo.MongoStoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.DB;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class MongoSchemaFactory implements SchemaFactory {

  static final Logger logger = LoggerFactory
      .getLogger(MongoSchemaFactory.class);

  private static final String DATABASES = "databases";

  private LoadingCache<String, List<String>> databases;
  private LoadingCache<String, List<String>> tableNameLoader;
  private final String schemaName;
  private final MongoStoragePlugin plugin;

  private final List<ServerAddress> addresses;
  private final MongoClientOptions options;

  public MongoSchemaFactory(MongoStoragePlugin schema, String schemaName)
      throws ExecutionSetupException, UnknownHostException {
    String connection = schema.getConfig().getConnection();

    this.plugin = schema;
    this.schemaName = schemaName;

    MongoClientURI clientURI = new MongoClientURI(connection);
    List<String> hosts = clientURI.getHosts();
    addresses = Lists.newArrayList();
    for (String host : hosts) {
      addresses.add(new ServerAddress(host));
    }
    options = clientURI.getOptions();

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
      return MongoCnxnManager.getClient(addresses, options).getDatabaseNames();
    }

  }

  private class TableNameLoader extends CacheLoader<String, List<String>> {

    @Override
    public List<String> load(String dbName) throws Exception {
      DB db = MongoCnxnManager.getClient(addresses, options).getDB(dbName);
      db.setReadPreference(ReadPreference.nearest());
      Set<String> collectionNames = db.getCollectionNames();
      return new ArrayList<>(collectionNames);
    }
  }

  @Override
  public void registerSchemas(UserSession session, SchemaPlus parent) {
    MongoSchema schema = new MongoSchema(schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class MongoSchema extends AbstractSchema {

    public MongoSchema(String name) {
      super(ImmutableList.<String> of(), name);
    }

    @Override
    public Schema getSubSchema(String name) {
      List<String> tables;
      try {
        tables = tableNameLoader.get(name);
        return new MongoDatabaseSchema(tables, this, name);
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
      return new DynamicDrillTable(plugin, schemaName, mongoScanSpec);
    }

    @Override
    public String getTypeName() {
      return MongoStoragePluginConfig.NAME;
    }
  }
}
