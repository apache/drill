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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.hive.DrillHiveMetaStoreClient;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveStoragePlugin;
import org.apache.drill.exec.store.hive.HiveStoragePluginConfig;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

public class HiveSchemaFactory implements SchemaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveSchemaFactory.class);

  // MetaStoreClient created using process user credentials
  private final DrillHiveMetaStoreClient processUserMetastoreClient;
  // MetasStoreClient created using SchemaConfig credentials
  private final LoadingCache<String, DrillHiveMetaStoreClient> metaStoreClientLoadingCache;

  private final HiveStoragePlugin plugin;
  private final String schemaName;
  private final HiveConf hiveConf;
  private final boolean isDrillImpersonationEnabled;
  private final boolean isHS2DoAsSet;

  public HiveSchemaFactory(final HiveStoragePlugin plugin, final String name, final HiveConf hiveConf) throws ExecutionSetupException {
    this.schemaName = name;
    this.plugin = plugin;

    this.hiveConf = hiveConf;
    isHS2DoAsSet = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS);
    isDrillImpersonationEnabled = plugin.getContext().getConfig().getBoolean(ExecConstants.IMPERSONATION_ENABLED);

    try {
      processUserMetastoreClient =
          DrillHiveMetaStoreClient.createNonCloseableClientWithCaching(hiveConf);
    } catch (MetaException e) {
      throw new ExecutionSetupException("Failure setting up Hive metastore client.", e);
    }

    metaStoreClientLoadingCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .maximumSize(5) // Up to 5 clients for impersonation-enabled.
        .removalListener(new RemovalListener<String, DrillHiveMetaStoreClient>() {
          @Override
          public void onRemoval(RemovalNotification<String, DrillHiveMetaStoreClient> notification) {
            DrillHiveMetaStoreClient client = notification.getValue();
            client.close();
          }
        })
        .build(new CacheLoader<String, DrillHiveMetaStoreClient>() {
          @Override
          public DrillHiveMetaStoreClient load(String userName) throws Exception {
            return DrillHiveMetaStoreClient.createClientWithAuthz(processUserMetastoreClient, hiveConf, userName);
          }
        });
  }

  /**
   * Does Drill needs to impersonate as user connected to Drill when reading data from Hive warehouse location?
   * @return True when both Drill impersonation and Hive impersonation are enabled.
   */
  private boolean needToImpersonateReadingData() {
    return isDrillImpersonationEnabled && isHS2DoAsSet;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    DrillHiveMetaStoreClient mClientForSchemaTree = processUserMetastoreClient;
    if (isDrillImpersonationEnabled) {
      try {
        mClientForSchemaTree = metaStoreClientLoadingCache.get(schemaConfig.getUserName());
      } catch (final ExecutionException e) {
        throw new IOException("Failure setting up Hive metastore client.", e);
      }
    }
    HiveSchema schema = new HiveSchema(schemaConfig, mClientForSchemaTree, schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class HiveSchema extends AbstractSchema {

    private final SchemaConfig schemaConfig;
    private final DrillHiveMetaStoreClient mClient;
    private HiveDatabaseSchema defaultSchema;

    public HiveSchema(final SchemaConfig schemaConfig, final DrillHiveMetaStoreClient mClient, final String name) {
      super(ImmutableList.<String>of(), name);
      this.schemaConfig = schemaConfig;
      this.mClient = mClient;
      getSubSchema("default");
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      try {
        List<String> dbs = mClient.getDatabases(schemaConfig.getIgnoreAuthErrors());
        if (!dbs.contains(name)) {
          logger.debug("Database '{}' doesn't exists in Hive storage '{}'", name, schemaName);
          return null;
        }
        HiveDatabaseSchema schema = getSubSchemaKnownExists(name);
        if (name.equals("default")) {
          this.defaultSchema = schema;
        }
        return schema;
      } catch (final TException e) {
        logger.warn("Failure while attempting to access HiveDatabase '{}'.", name, e.getCause());
        return null;
      }
    }

    /** Help method to get subschema when we know it exists (already checks the existence) */
    private HiveDatabaseSchema getSubSchemaKnownExists(String name) {
      HiveDatabaseSchema schema = new HiveDatabaseSchema(this, name, mClient, schemaConfig);
      return schema;
    }

    void setHolder(SchemaPlus plusOfThis) {
      for (String s : getSubSchemaNames()) {
        plusOfThis.add(s, getSubSchemaKnownExists(s));
      }
    }

    @Override
    public boolean showInInformationSchema() {
      return false;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      try {
        List<String> dbs = mClient.getDatabases(schemaConfig.getIgnoreAuthErrors());
        return Sets.newHashSet(dbs);
      } catch (final TException e) {
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

    DrillTable getDrillTable(String dbName, String t) {
      HiveReadEntry entry = getSelectionBaseOnName(dbName, t);
      if (entry == null) {
        return null;
      }

      final String userToImpersonate = needToImpersonateReadingData() ? schemaConfig.getUserName() :
          ImpersonationUtil.getProcessUserName();

      if (entry.getJdbcTableType() == TableType.VIEW) {
        return new DrillHiveViewTable(schemaName, plugin, userToImpersonate, entry);
      } else {
        return new DrillHiveTable(schemaName, plugin, userToImpersonate, entry);
      }
    }

    HiveReadEntry getSelectionBaseOnName(String dbName, String t) {
      if (dbName == null) {
        dbName = "default";
      }
      try{
        return mClient.getHiveReadEntry(dbName, t, schemaConfig.getIgnoreAuthErrors());
      }catch(final TException e) {
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
