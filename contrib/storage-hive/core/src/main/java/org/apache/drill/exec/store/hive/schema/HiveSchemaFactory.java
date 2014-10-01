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
package org.apache.drill.exec.store.hive.schema;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveStoragePlugin;
import org.apache.drill.exec.store.hive.HiveStoragePluginConfig;
import org.apache.drill.exec.store.hive.HiveTable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class HiveSchemaFactory implements SchemaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveSchemaFactory.class);

  private static final String DATABASES = "databases";

  private final HiveMetaStoreClient mClient;
  private LoadingCache<String, List<String>> databases;
  private LoadingCache<String, List<String>> tableNameLoader;
  private LoadingCache<String, LoadingCache<String, HiveReadEntry>> tableLoaders;
  private HiveStoragePlugin plugin;
  private final String schemaName;
  private final Map<String, String> hiveConfigOverride;

  public HiveSchemaFactory(HiveStoragePlugin plugin, String name, Map<String, String> hiveConfigOverride) throws ExecutionSetupException {
    this.schemaName = name;
    this.plugin = plugin;

    this.hiveConfigOverride = hiveConfigOverride;
    HiveConf hiveConf = new HiveConf();
    if (hiveConfigOverride != null) {
      for (Map.Entry<String, String> entry : hiveConfigOverride.entrySet()) {
        hiveConf.set(entry.getKey(), entry.getValue());
      }
    }

    try {
      this.mClient = new HiveMetaStoreClient(hiveConf);
    } catch (MetaException e) {
      throw new ExecutionSetupException("Failure setting up Hive metastore client.", e);
    }

    databases = CacheBuilder //
        .newBuilder() //
        .expireAfterAccess(1, TimeUnit.MINUTES) //
        .build(new DatabaseLoader());

    tableNameLoader = CacheBuilder //
        .newBuilder() //
        .expireAfterAccess(1, TimeUnit.MINUTES) //
        .build(new TableNameLoader());

    tableLoaders = CacheBuilder //
        .newBuilder() //
        .expireAfterAccess(4, TimeUnit.HOURS) //
        .maximumSize(20) //
        .build(new TableLoaderLoader());
  }

  private class TableNameLoader extends CacheLoader<String, List<String>> {

    @Override
    public List<String> load(String dbName) throws Exception {
      try {
        return mClient.getAllTables(dbName);
      } catch (TException e) {
        logger.warn("Failure while attempting to get hive tables", e);
        mClient.reconnect();
        return mClient.getAllTables(dbName);
      }
    }

  }

  private class DatabaseLoader extends CacheLoader<String, List<String>> {

    @Override
    public List<String> load(String key) throws Exception {
      if (!DATABASES.equals(key)) {
        throw new UnsupportedOperationException();
      }
      try {
        return mClient.getAllDatabases();
      } catch (TException e) {
        logger.warn("Failure while attempting to get hive tables", e);
        mClient.reconnect();
        return mClient.getAllDatabases();
      }
    }
  }

  private class TableLoaderLoader extends CacheLoader<String, LoadingCache<String, HiveReadEntry>> {

    @Override
    public LoadingCache<String, HiveReadEntry> load(String key) throws Exception {
      return CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.MINUTES).build(new TableLoader(key));
    }

  }

  private class TableLoader extends CacheLoader<String, HiveReadEntry> {

    private final String dbName;

    public TableLoader(String dbName) {
      super();
      this.dbName = dbName;
    }

    @Override
    public HiveReadEntry load(String key) throws Exception {
      Table t = null;
      try {
        t = mClient.getTable(dbName, key);
      } catch (TException e) {
        mClient.reconnect();
        t = mClient.getTable(dbName, key);
      }

      if (t == null) {
        throw new UnknownTableException(String.format("Unable to find table '%s'.", key));
      }

      List<Partition> partitions = null;
      try {
        partitions = mClient.listPartitions(dbName, key, Short.MAX_VALUE);
      } catch (TException e) {
        mClient.reconnect();
        partitions = mClient.listPartitions(dbName, key, Short.MAX_VALUE);
      }

      List<HiveTable.HivePartition> hivePartitions = Lists.newArrayList();
      for (Partition part : partitions) {
        hivePartitions.add(new HiveTable.HivePartition(part));
      }

      if (hivePartitions.size() == 0) {
        hivePartitions = null;
      }
      return new HiveReadEntry(new HiveTable(t), hivePartitions, hiveConfigOverride);

    }

  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    HiveSchema schema = new HiveSchema(schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class HiveSchema extends AbstractSchema {

    private HiveDatabaseSchema defaultSchema;

    public HiveSchema(String name) {
      super(ImmutableList.<String>of(), name);
      getSubSchema("default");
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      List<String> tables;
      try {
        List<String> dbs = databases.get(DATABASES);
        if (!dbs.contains(name)) {
          logger.debug(String.format("Database '%s' doesn't exists in Hive storage '%s'", name, schemaName));
          return null;
        }
        tables = tableNameLoader.get(name);
        HiveDatabaseSchema schema = new HiveDatabaseSchema(tables, this, name);
        if (name.equals("default")) {
          this.defaultSchema = schema;
        }
        return schema;
      } catch (ExecutionException e) {
        logger.warn("Failure while attempting to access HiveDatabase '{}'.", name, e.getCause());
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
        logger.warn("Failure while getting Hive database list.", e);
      }
      return super.getSubSchemaNames();
    }

    @Override
    public org.apache.calcite.schema.Table getTable(String name) {
      if (defaultSchema == null) {
        return super.getTable(name);
      }
      return defaultSchema.getTable(name);
    }

    @Override
    public Set<String> getTableNames() {
      if (defaultSchema == null) {
        return super.getTableNames();
      }
      return defaultSchema.getTableNames();
    }

    List<String> getTableNames(String dbName) {
      try{
        return tableNameLoader.get(dbName);
      } catch (ExecutionException e) {
        logger.warn("Failure while loading table names for database '{}'.", dbName, e.getCause());
        return Collections.emptyList();
      }
    }

    DrillTable getDrillTable(String dbName, String t) {
      HiveReadEntry entry = getSelectionBaseOnName(dbName, t);
      if (entry == null) {
        return null;
      }

      if (entry.getJdbcTableType() == TableType.VIEW) {
        return new DrillHiveViewTable(schemaName, plugin, entry);
      } else {
        return new DrillHiveTable(schemaName, plugin, entry);
      }
    }

    HiveReadEntry getSelectionBaseOnName(String dbName, String t) {
      if (dbName == null) {
        dbName = "default";
      }
      try{
        return tableLoaders.get(dbName).get(t);
      }catch(ExecutionException e) {
        logger.warn("Exception occurred while trying to read table. {}.{}", dbName, t, e.getCause());
        return null;
      }
    }

    @Override
    public AbstractSchema getDefaultSchema() {
      return defaultSchema;
    }

    @Override
    public String getTypeName() {
      return HiveStoragePluginConfig.NAME;
    }

  }

}
