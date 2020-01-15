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
package org.apache.drill.exec.store.cassandra;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;


public class CassandraSchemaFactory implements SchemaFactory {

  static final Logger logger = LoggerFactory.getLogger(CassandraSchemaFactory.class);

  private static final String DATABASES = "keyspaces";

  private LoadingCache<String, List<String>> keyspaceCache;

  private LoadingCache<String, List<String>> tableCache;

  private final String schemaName;

  private final CassandraStoragePlugin plugin;

  private final Cluster cluster;

  public CassandraSchemaFactory(CassandraStoragePlugin schema, String schemaName) {
    List<String> hosts = schema.getConfig().getHosts();
    int port = schema.getConfig().getPort();

    this.plugin = schema;
    this.schemaName = schemaName;

    Cluster.Builder builder = Cluster.builder();
    for (String host : hosts) {
      builder = builder.addContactPoint(host);
    }
    builder = builder.withPort(port).withoutJMXReporting();
    cluster = builder.build();

    keyspaceCache = CacheBuilder //
      .newBuilder() //
      .expireAfterAccess(1, TimeUnit.MINUTES) //
      .build(new KeyspaceLoader());

    tableCache = CacheBuilder //
      .newBuilder() //
      .expireAfterAccess(1, TimeUnit.MINUTES) //
      .build(new TableNameLoader());
  }


  /**
   * Utility class for fetching all the key spaces in cluster.
   */
  private class KeyspaceLoader extends CacheLoader<String, List<String>> {

    @Override
    public List<String> load(String key) {
      if (!DATABASES.equals(key)) {
        throw new UnsupportedOperationException();
      }
      List<KeyspaceMetadata> keyspaces = cluster.getMetadata().getKeyspaces();
      List<String> keys = Lists.newArrayList();
      for (KeyspaceMetadata k : keyspaces) {
        keys.add(k.getName());
      }
      return keys;
    }
  }

  /**
   * Utility class for populating all tables in a provided key space.
   */
  private class TableNameLoader extends CacheLoader<String, List<String>> {

    @Override
    public List<String> load(String keyspace) {
      Collection<TableMetadata> tables = cluster.getMetadata().getKeyspace(keyspace).getTables();
      List<String> tabs = Lists.newArrayList();
      for (TableMetadata t : tables) {
        tabs.add(t.getName());
      }
      return tabs;
    }
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    CassandraSchema schema = new CassandraSchema(schemaName);
    SchemaPlus schemaPlus = parent.add(schemaName, schema);
    schema.setHolder(schemaPlus);
  }

  class CassandraSchema extends AbstractSchema {

    public CassandraSchema(String name) {
      super(ImmutableList.<String>of(), name);
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      List<String> tables;
      try {
        tables = tableCache.get(name);
        return new CassandraDatabaseSchema(tables, this, name);
      } catch (ExecutionException e) {
        logger.warn("Failure while attempting to access Cassandra DataBase '{}'.", name, e.getCause());
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
        List<String> dbs = keyspaceCache.get(DATABASES);
        return Sets.newHashSet(dbs);
      } catch (ExecutionException e) {
        logger.warn("Failure while getting Cassandra keyspace list.", e);
        return Collections.emptySet();
      }
    }

    List<String> getTableNames(String dbName) {
      try {
        return tableCache.get(dbName);
      } catch (ExecutionException e) {
        logger.warn("Failure while loading table names for keyspace '{}'.", dbName, e.getCause());
        return Collections.emptyList();
      }
    }

    DrillTable getDrillTable(String dbName, String tableName) {
      CassandraScanSpec cassandraScanSpec = new CassandraScanSpec(dbName, tableName);
      return new DynamicDrillTable(plugin, schemaName, cassandraScanSpec);
    }

    @Override
    public String getTypeName() {
      return CassandraStoragePluginConfig.NAME;
    }
  }
}