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
package org.apache.drill.exec.store.hive;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Override HiveMetaStoreClient to provide additional capabilities such as caching, reconnecting with user
 * credentials and higher level APIs to get the metadata in form that Drill needs directly.
 */
public abstract class DrillHiveMetaStoreClient extends HiveMetaStoreClient {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillHiveMetaStoreClient.class);

  protected final Map<String, String> hiveConfigOverride;

  /**
   * Create a DrillHiveMetaStoreClient for cases where:
   *   1. Drill impersonation is enabled and
   *   2. either storage (in remote HiveMetaStore server) or SQL standard based authorization (in Hive storage plugin)
   *      is enabled
   * @param hiveConf Conf including authorization configuration
   * @param hiveConfigOverride
   * @param userName User who is trying to access the Hive metadata
   * @param ignoreAuthzErrors When browsing info schema, we want to ignore permission denied errors. If a permission
   *                          denied error occurs while accessing metadata for an object, it will not be shown in the
   *                          info schema.
   * @return
   * @throws MetaException
   */
  public static DrillHiveMetaStoreClient createClientWithAuthz(final HiveConf hiveConf,
      final Map<String, String> hiveConfigOverride, final String userName, final boolean ignoreAuthzErrors)
      throws MetaException {
    try {
      final UserGroupInformation ugiForRpc; // UGI credentials to use for RPC communication with Hive MetaStore server
      if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
        // If the user impersonation is disabled in Hive storage plugin (not Drill impersonation), use the process
        // user UGI credentials.
        ugiForRpc = ImpersonationUtil.getProcessUserUGI();
      } else {
        ugiForRpc = ImpersonationUtil.createProxyUgi(userName);
      }
      return ugiForRpc.doAs(new PrivilegedExceptionAction<DrillHiveMetaStoreClient>() {
        @Override
        public DrillHiveMetaStoreClient run() throws Exception {
          return new HiveClientWithAuthz(hiveConf, hiveConfigOverride, ugiForRpc, userName, ignoreAuthzErrors);
        }
      });
    } catch (final Exception e) {
      throw new DrillRuntimeException("Failure setting up HiveMetaStore client.", e);
    }
  }

  /**
   * Create a DrillMetaStoreClient that can be shared across multiple users. This is created when impersonation is
   * disabled.
   * @param hiveConf
   * @param hiveConfigOverride
   * @return
   * @throws MetaException
   */
  public static DrillHiveMetaStoreClient createNonCloseableClientWithCaching(final HiveConf hiveConf,
      final Map<String, String> hiveConfigOverride) throws MetaException {
    return new NonCloseableHiveClientWithCaching(hiveConf, hiveConfigOverride);
  }

  private DrillHiveMetaStoreClient(final HiveConf hiveConf, final Map<String, String> hiveConfigOverride)
      throws MetaException {
    super(hiveConf);
    this.hiveConfigOverride = hiveConfigOverride;
  }


  /**
   * Higher level API that returns the databases in Hive.
   * @return
   * @throws TException
   */
  public abstract List<String> getDatabases() throws TException;

  /**
   * Higher level API that returns the tables in given database.
   * @param dbName
   * @return
   * @throws TException
   */
  public abstract List<String> getTableNames(final String dbName) throws TException;

  /**
   * Higher level API that returns the {@link HiveReadEntry} for given database and table.
   * @param dbName
   * @param tableName
   * @return
   * @throws TException
   */
  public abstract HiveReadEntry getHiveReadEntry(final String dbName, final String tableName) throws TException;

  /** Helper method which gets database. Retries once if the first call to fetch the metadata fails */
  protected static List<String> getDatabasesHelper(final IMetaStoreClient mClient) throws TException {
    try {
      return mClient.getAllDatabases();
    } catch (TException e) {
      logger.warn("Failure while attempting to get hive databases", e);
      mClient.reconnect();
      return mClient.getAllDatabases();
    }
  }

  /** Helper method which gets tables in a database. Retries once if the first call to fetch the metadata fails */
  protected static List<String> getTableNamesHelper(final IMetaStoreClient mClient, final String dbName)
      throws TException {
    try {
      return mClient.getAllTables(dbName);
    } catch (TException e) {
      logger.warn("Failure while attempting to get hive tables", e);
      mClient.reconnect();
      return mClient.getAllTables(dbName);
    }
  }

  /** Helper method which gets table metadata. Retries once if the first call to fetch the metadata fails */
  protected static HiveReadEntry getHiveReadEntryHelper(final IMetaStoreClient mClient, final String dbName,
      final String tableName, final Map<String, String> hiveConfigOverride) throws TException {
    Table t = null;
    try {
      t = mClient.getTable(dbName, tableName);
    } catch (TException e) {
      mClient.reconnect();
      t = mClient.getTable(dbName, tableName);
    }

    if (t == null) {
      throw new UnknownTableException(String.format("Unable to find table '%s'.", tableName));
    }

    List<Partition> partitions;
    try {
      partitions = mClient.listPartitions(dbName, tableName, (short) -1);
    } catch (TException e) {
      mClient.reconnect();
      partitions = mClient.listPartitions(dbName, tableName, (short) -1);
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

  /**
   * HiveMetaStoreClient to create and maintain (reconnection cases) connection to Hive metastore with given user
   * credentials and check authorization privileges if set.
   */
  private static class HiveClientWithAuthz extends DrillHiveMetaStoreClient {
    private final UserGroupInformation ugiForRpc;
    private final boolean ignoreAuthzErrors;
    private HiveAuthorizationHelper authorizer;

    private HiveClientWithAuthz(final HiveConf hiveConf, final Map<String, String> hiveConfigOverride,
        final UserGroupInformation ugiForRpc, final String userName, final boolean ignoreAuthzErrors)
        throws TException {
      super(hiveConf, hiveConfigOverride);
      this.ugiForRpc = ugiForRpc;
      this.ignoreAuthzErrors = ignoreAuthzErrors;
      this.authorizer = new HiveAuthorizationHelper(this, hiveConf, userName);
    }

    @Override
    public void reconnect() throws MetaException {
      try {
        ugiForRpc.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            reconnectSuper();
            return null;
          }
        });
      } catch (final InterruptedException | IOException e) {
        throw new DrillRuntimeException("Failed to reconnect to HiveMetaStore: " + e.getMessage(), e);
      }
    }

    private void reconnectSuper() throws MetaException {
      super.reconnect();
    }

    public List<String> getDatabases() throws TException {
      try {
        authorizer.authorizeShowDatabases();
      } catch (final HiveAccessControlException e) {
        if (ignoreAuthzErrors) {
          return Collections.emptyList();
        }
        throw UserException.permissionError(e).build(logger);
      }
      return getDatabasesHelper(this);
    }

    public List<String> getTableNames(final String dbName) throws TException {
      try {
        authorizer.authorizeShowTables(dbName);
      } catch (final HiveAccessControlException e) {
        if (ignoreAuthzErrors) {
          return Collections.emptyList();
        }
        throw UserException.permissionError(e).build(logger);
      }
      return getTableNamesHelper(this, dbName);
    }

    public HiveReadEntry getHiveReadEntry(final String dbName, final String tableName) throws TException {
      try {
        authorizer.authorizeReadTable(dbName, tableName);
      } catch (final HiveAccessControlException e) {
        if (!ignoreAuthzErrors) {
          throw UserException.permissionError(e).build(logger);
        }
      }
      return getHiveReadEntryHelper(this, dbName, tableName, hiveConfigOverride);
    }
  }

  /**
   * HiveMetaStoreClient that provides a shared MetaStoreClient implementation with caching.
   */
  private static class NonCloseableHiveClientWithCaching extends DrillHiveMetaStoreClient {
    private final LoadingCache<String, List<String>> databases;
    private final LoadingCache<String, List<String>> tableNameLoader;
    private final LoadingCache<String, LoadingCache<String, HiveReadEntry>> tableLoaders;

    private NonCloseableHiveClientWithCaching(final HiveConf hiveConf,
        final Map<String, String> hiveConfigOverride) throws MetaException {
      super(hiveConf, hiveConfigOverride);

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

    @Override
    public List<String> getDatabases() throws TException {
      try {
        return databases.get("databases");
      } catch (final ExecutionException e) {
        throw new TException(e);
      }
    }

    @Override
    public List<String> getTableNames(final String dbName) throws TException {
      try {
        return tableNameLoader.get(dbName);
      } catch (final ExecutionException e) {
        throw new TException(e);
      }
    }

    @Override
    public HiveReadEntry getHiveReadEntry(final String dbName, final String tableName) throws TException {
      try {
        return tableLoaders.get(dbName).get(tableName);
      } catch (final ExecutionException e) {
        throw new TException(e);
      }
    }

    @Override
    public void close() {
      // No-op.
    }

    private class DatabaseLoader extends CacheLoader<String, List<String>> {
      @Override
      public List<String> load(String key) throws Exception {
        if (!"databases".equals(key)) {
          throw new UnsupportedOperationException();
        }
        synchronized (NonCloseableHiveClientWithCaching.this) {
          return getDatabasesHelper(NonCloseableHiveClientWithCaching.this);
        }
      }
    }

    private class TableNameLoader extends CacheLoader<String, List<String>> {
      @Override
      public List<String> load(String dbName) throws Exception {
        synchronized (NonCloseableHiveClientWithCaching.this) {
          return getTableNamesHelper(NonCloseableHiveClientWithCaching.this, dbName);
        }
      }
    }

    private class TableLoaderLoader extends CacheLoader<String, LoadingCache<String, HiveReadEntry>> {
      @Override
      public LoadingCache<String, HiveReadEntry> load(String key) throws Exception {
        return CacheBuilder
            .newBuilder()
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build(new TableLoader(key));
      }
    }

    private class TableLoader extends CacheLoader<String, HiveReadEntry> {
      private final String dbName;

      public TableLoader(final String dbName) {
        this.dbName = dbName;
      }

      @Override
      public HiveReadEntry load(String key) throws Exception {
        synchronized (NonCloseableHiveClientWithCaching.this) {
          return getHiveReadEntryHelper(NonCloseableHiveClientWithCaching.this, dbName, key, hiveConfigOverride);
        }
      }
    }
  }
}
