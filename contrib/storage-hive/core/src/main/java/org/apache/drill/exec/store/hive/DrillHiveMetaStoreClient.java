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
package org.apache.drill.exec.store.hive;

import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Override HiveMetaStoreClient to provide additional capabilities such as caching, reconnecting with user
 * credentials and higher level APIs to get the metadata in form that Drill needs directly.
 */
public abstract class DrillHiveMetaStoreClient extends HiveMetaStoreClient {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillHiveMetaStoreClient.class);

  public final String HIVE_METASTORE_CACHE_TTL = "hive.metastore.cache-ttl-seconds";
  public final String HIVE_METASTORE_CACHE_EXPIRE = "hive.metastore.cache-expire-after";
  public final String HIVE_METASTORE_CACHE_EXPIRE_AFTER_WRITE = "write";
  public final String HIVE_METASTORE_CACHE_EXPIRE_AFTER_ACCESS = "access";

  protected final LoadingCache<String, List<String>> databases;
  protected final LoadingCache<String, List<String>> tableNameLoader;
  protected final LoadingCache<TableName, HiveReadEntry> tableLoaders;


  /**
   * Create a DrillHiveMetaStoreClient for cases where:
   *   1. Drill impersonation is enabled and
   *   2. either storage (in remote HiveMetaStore server) or SQL standard based authorization (in Hive storage plugin)
   *      is enabled
   * @param processUserMetaStoreClient MetaStoreClient of process user. Useful for generating the delegation tokens when
   *                                   SASL (KERBEROS or custom SASL implementations) is enabled.
   * @param hiveConf Conf including authorization configuration
   * @param userName User who is trying to access the Hive metadata
   * @return
   * @throws MetaException
   */
  public static DrillHiveMetaStoreClient createClientWithAuthz(final DrillHiveMetaStoreClient processUserMetaStoreClient,
      final HiveConf hiveConf, final String userName) throws MetaException {
    try {
      boolean delegationTokenGenerated = false;

      final UserGroupInformation ugiForRpc; // UGI credentials to use for RPC communication with Hive MetaStore server
      if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
        // If the user impersonation is disabled in Hive storage plugin (not Drill impersonation), use the process
        // user UGI credentials.
        ugiForRpc = ImpersonationUtil.getProcessUserUGI();
      } else {
        ugiForRpc = ImpersonationUtil.createProxyUgi(userName);
        if (hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL)) {
          // When SASL is enabled for proxy user create a delegation token. Currently HiveMetaStoreClient can create
          // client transport for proxy users only when the authentication mechanims is DIGEST (through use of
          // delegation tokens).
          String delegationToken = processUserMetaStoreClient.getDelegationToken(userName, userName);
          try {
            Utils.setTokenStr(ugiForRpc, delegationToken, HiveClientWithAuthzWithCaching.DRILL2HMS_TOKEN);
          } catch (IOException e) {
            throw new DrillRuntimeException("Couldn't setup delegation token in the UGI for Hive MetaStoreClient", e);
          }
          delegationTokenGenerated = true;
        }
      }

      final HiveConf hiveConfForClient;
      if (delegationTokenGenerated) {
        hiveConfForClient = new HiveConf(hiveConf);
        hiveConfForClient.set("hive.metastore.token.signature", HiveClientWithAuthzWithCaching.DRILL2HMS_TOKEN);
      } else {
        hiveConfForClient = hiveConf;
      }

      return ugiForRpc.doAs(new PrivilegedExceptionAction<DrillHiveMetaStoreClient>() {
        @Override
        public DrillHiveMetaStoreClient run() throws Exception {
          return new HiveClientWithAuthzWithCaching(hiveConfForClient, ugiForRpc, userName);
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
   * @return
   * @throws MetaException
   */
  public static DrillHiveMetaStoreClient createCloseableClientWithCaching(final HiveConf hiveConf)
      throws MetaException {
    return new HiveClientWithCaching(hiveConf);
  }

  private DrillHiveMetaStoreClient(final HiveConf hiveConf) throws MetaException {
    super(hiveConf);

    int hmsCacheTTL = 60; // default is 60 seconds
    boolean expireAfterWrite = true; // default is expire after write.

    final String ttl = hiveConf.get(HIVE_METASTORE_CACHE_TTL);
    if (!Strings.isNullOrEmpty(ttl)) {
      hmsCacheTTL = Integer.valueOf(ttl);
      logger.warn("Hive metastore cache ttl is set to {} seconds.", hmsCacheTTL);
    }

    final String expiry = hiveConf.get(HIVE_METASTORE_CACHE_EXPIRE);
    if (!Strings.isNullOrEmpty(expiry)) {
      if (expiry.equalsIgnoreCase(HIVE_METASTORE_CACHE_EXPIRE_AFTER_WRITE)) {
        expireAfterWrite = true;
      } else if (expiry.equalsIgnoreCase(HIVE_METASTORE_CACHE_EXPIRE_AFTER_ACCESS)) {
        expireAfterWrite = false;
      }
      logger.warn("Hive metastore cache expire policy is set to {}", expireAfterWrite? "expireAfterWrite" : "expireAfterAccess");
    }

    final CacheBuilder<Object, Object> cacheBuilder = CacheBuilder
        .newBuilder();

    if (expireAfterWrite) {
      cacheBuilder.expireAfterWrite(hmsCacheTTL, TimeUnit.SECONDS);
    } else {
      cacheBuilder.expireAfterAccess(hmsCacheTTL, TimeUnit.SECONDS);
    }

    databases = cacheBuilder.build(new DatabaseLoader());
    tableNameLoader = cacheBuilder.build(new TableNameLoader());
    tableLoaders = cacheBuilder.build(new TableLoader());

  }

  /**
   * Higher level API that returns the databases in Hive.
   * @return
   * @throws TException
   */
  public abstract List<String> getDatabases(boolean ignoreAuthzErrors) throws TException;

  /**
   * Higher level API that returns the tables in given database.
   * @param dbName
   * @return
   * @throws TException
   */
  public abstract List<String> getTableNames(final String dbName, boolean ignoreAuthzErrors) throws TException;

  /**
   * Higher level API that returns the {@link HiveReadEntry} for given database and table.
   * @param dbName
   * @param tableName
   * @return
   * @throws TException
   */
  public abstract HiveReadEntry getHiveReadEntry(final String dbName, final String tableName, boolean ignoreAuthzErrors) throws TException;

  /** Helper method which gets database. Retries once if the first call to fetch the metadata fails */
  protected static List<String> getDatabasesHelper(final IMetaStoreClient mClient) throws TException {
    try {
      return mClient.getAllDatabases();
    } catch (MetaException e) {
      /*
         HiveMetaStoreClient is encapsulating both the MetaException/TExceptions inside MetaException.
         Since we don't have good way to differentiate, we will close older connection and retry once.
         This is only applicable for getAllTables and getAllDatabases method since other methods are
         properly throwing correct exceptions.
      */
      logger.warn("Failure while attempting to get hive databases. Retries once.", e);
      try {
        mClient.close();
      } catch (Exception ex) {
        logger.warn("Failure while attempting to close existing hive metastore connection. May leak connection.", ex);
      }

      // Attempt to reconnect. If this is a secure connection, this will fail due
      // to the invalidation of the security token. In that case, throw the original
      // exception and let a higher level clean up. Ideally we'd get a new token
      // here, but doing so requires the use of a different connection, and that
      // one has also become invalid. This code needs a rework; this is just a
      // work-around.

      try {
        mClient.reconnect();
      } catch (Exception e1) {
        throw e;
      }
      return mClient.getAllDatabases();
    }
  }

  /** Helper method which gets tables in a database. Retries once if the first call to fetch the metadata fails */
  protected static List<String> getTableNamesHelper(final IMetaStoreClient mClient, final String dbName)
      throws TException {
    try {
      return mClient.getAllTables(dbName);
    } catch (MetaException e) {
      /*
         HiveMetaStoreClient is encapsulating both the MetaException/TExceptions inside MetaException.
         Since we don't have good way to differentiate, we will close older connection and retry once.
         This is only applicable for getAllTables and getAllDatabases method since other methods are
         properly throwing correct exceptions.
      */
      logger.warn("Failure while attempting to get hive tables. Retries once.", e);
      try {
        mClient.close();
      } catch (Exception ex) {
        logger.warn("Failure while attempting to close existing hive metastore connection. May leak connection.", ex);
      }
      mClient.reconnect();
      return mClient.getAllTables(dbName);
    }
  }

  public static List<Table> getTablesByNamesByBulkLoadHelper(
      final HiveMetaStoreClient mClient, final List<String> tableNames, final String schemaName,
      final int bulkSize) {
    final int totalTables = tableNames.size();
    final List<org.apache.hadoop.hive.metastore.api.Table> tables = Lists.newArrayList();

    // In each round, Drill asks for a sub-list of all the requested tables
    for (int fromIndex = 0; fromIndex < totalTables; fromIndex += bulkSize) {
      final int toIndex = Math.min(fromIndex + bulkSize, totalTables);
      final List<String> eachBulkofTableNames = tableNames.subList(fromIndex, toIndex);
      List<org.apache.hadoop.hive.metastore.api.Table> eachBulkofTables;
      // Retries once if the first call to fetch the metadata fails
      try {
        eachBulkofTables = DrillHiveMetaStoreClient.getTableObjectsByNameHelper(mClient, schemaName, eachBulkofTableNames);
      } catch (Exception e) {
        logger.warn("Exception occurred while trying to read tables from {}: {}", schemaName, e.getCause());
        return ImmutableList.of();
      }
      tables.addAll(eachBulkofTables);
    }
    return tables;
  }

  /** Helper method which gets table metadata. Retries once if the first call to fetch the metadata fails */
  protected static HiveReadEntry getHiveReadEntryHelper(final IMetaStoreClient mClient, final String dbName,
      final String tableName) throws TException {
    Table table = null;
    try {
      table = mClient.getTable(dbName, tableName);
    } catch (MetaException | NoSuchObjectException e) {
      throw e;
    } catch (TException e) {
      logger.warn("Failure while attempting to get hive table. Retries once. ", e);
      try {
        mClient.close();
      } catch (Exception ex) {
        logger.warn("Failure while attempting to close existing hive metastore connection. May leak connection.", ex);
      }
      mClient.reconnect();
      table = mClient.getTable(dbName, tableName);
    }

    if (table == null) {
      throw new UnknownTableException(String.format("Unable to find table '%s'.", tableName));
    }

    List<Partition> partitions;
    try {
      partitions = mClient.listPartitions(dbName, tableName, (short) -1);
    } catch (NoSuchObjectException | MetaException e) {
      throw e;
    } catch (TException e) {
      logger.warn("Failure while attempting to get hive partitions. Retries once. ", e);
      try {
        mClient.close();
      } catch (Exception ex) {
        logger.warn("Failure while attempting to close existing hive metastore connection. May leak connection.", ex);
      }
      mClient.reconnect();
      partitions = mClient.listPartitions(dbName, tableName, (short) -1);
    }

    List<HiveTableWrapper.HivePartitionWrapper> hivePartitionWrappers = Lists.newArrayList();
    HiveTableWithColumnCache hiveTable = new HiveTableWithColumnCache(table, new ColumnListsCache(table));
    for (Partition partition : partitions) {
      hivePartitionWrappers.add(createPartitionWithSpecColumns(hiveTable, partition));
    }

    if (hivePartitionWrappers.isEmpty()) {
      hivePartitionWrappers = null;
    }

    return new HiveReadEntry(new HiveTableWrapper(hiveTable), hivePartitionWrappers);
  }

  /**
   * Helper method which stores partition columns in table columnListCache. If table columnListCache has exactly the
   * same columns as partition, in partition stores columns index that corresponds to identical column list.
   * If table columnListCache hasn't such column list, the column list adds to table columnListCache and in partition
   * stores columns index that corresponds to column list.
   *
   * @param table     hive table instance
   * @param partition partition instance
   * @return hive partition wrapper
   */
  public static HiveTableWrapper.HivePartitionWrapper createPartitionWithSpecColumns(HiveTableWithColumnCache table, Partition partition) {
    int listIndex = table.getColumnListsCache().addOrGet(partition.getSd().getCols());
    HivePartition hivePartition = new HivePartition(partition, listIndex);
    HiveTableWrapper.HivePartitionWrapper hivePartitionWrapper = new HiveTableWrapper.HivePartitionWrapper(hivePartition);
    return hivePartitionWrapper;
  }

  /**
   * Help method which gets hive tables for a given schema|DB name and a list of table names.
   * Retries once if the first call fails with TExcption other than connection-lost problems.
   * @param mClient
   * @param schemaName
   * @param tableNames
   * @return  list of hive table instances.
   **/
  public static List<Table> getTableObjectsByNameHelper(final HiveMetaStoreClient mClient, final String schemaName,
      final List<String> tableNames) throws TException {
    try {
      return mClient.getTableObjectsByName(schemaName, tableNames);
    } catch (MetaException | InvalidOperationException | UnknownDBException e) {
      throw e;
    } catch (TException e) {
      logger.warn("Failure while attempting to get tables by names. Retries once. ", e);
      try {
        mClient.close();
      } catch (Exception ex) {
        logger.warn("Failure while attempting to close existing hive metastore connection. May leak connection.", ex);
      }
      mClient.reconnect();
      return mClient.getTableObjectsByName(schemaName, tableNames);
    }
  }

  /**
   * HiveMetaStoreClient to create and maintain (reconnection cases) connection to Hive metastore with given user
   * credentials and check authorization privileges if set.
   */
  private static class HiveClientWithAuthzWithCaching extends DrillHiveMetaStoreClient {
    public static final String DRILL2HMS_TOKEN = "DrillDelegationTokenForHiveMetaStoreServer";

    private final UserGroupInformation ugiForRpc;
    private HiveAuthorizationHelper authorizer;

    private HiveClientWithAuthzWithCaching(final HiveConf hiveConf, final UserGroupInformation ugiForRpc,
        final String userName) throws TException {
      super(hiveConf);
      this.ugiForRpc = ugiForRpc;
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

    @Override
    public List<String> getDatabases(boolean ignoreAuthzErrors) throws TException {
      try {
        authorizer.authorizeShowDatabases();
      } catch (final HiveAccessControlException e) {
        if (ignoreAuthzErrors) {
          return Collections.emptyList();
        }
        throw UserException.permissionError(e).build(logger);
      }

      try {
        return databases.get("databases");
      } catch (final ExecutionException e) {
        throw new TException(e);
      }
    }

    @Override
    public List<String> getTableNames(final String dbName, boolean ignoreAuthzErrors) throws TException {
      try {
        authorizer.authorizeShowTables(dbName);
      } catch (final HiveAccessControlException e) {
        if (ignoreAuthzErrors) {
          return Collections.emptyList();
        }
        throw UserException.permissionError(e).build(logger);
      }

      try {
        return tableNameLoader.get(dbName);
      } catch (final ExecutionException e) {
        throw new TException(e);
      }
    }

    @Override
    public HiveReadEntry getHiveReadEntry(final String dbName, final String tableName, boolean ignoreAuthzErrors) throws TException {
      try {
        authorizer.authorizeReadTable(dbName, tableName);
      } catch (final HiveAccessControlException e) {
        if (!ignoreAuthzErrors) {
          throw UserException.permissionError(e).build(logger);
        }
      }

      try {
        return tableLoaders.get(TableName.table(dbName,tableName));
      } catch (final ExecutionException e) {
        throw new TException(e);
      }
    }

  }

  /**
   * HiveMetaStoreClient that provides a shared MetaStoreClient implementation with caching.
   */
  private static class HiveClientWithCaching extends DrillHiveMetaStoreClient {
    private HiveClientWithCaching(final HiveConf hiveConf) throws MetaException {
      super(hiveConf);
    }

    @Override
    public List<String> getDatabases(boolean ignoreAuthzErrors) throws TException {
      try {
        return databases.get("databases");
      } catch (final ExecutionException e) {
        throw new TException(e);
      }
    }

    @Override
    public List<String> getTableNames(final String dbName, boolean ignoreAuthzErrors) throws TException {
      try {
        return tableNameLoader.get(dbName);
      } catch (final ExecutionException e) {
        throw new TException(e);
      }
    }

    @Override
    public HiveReadEntry getHiveReadEntry(final String dbName, final String tableName, boolean ignoreAuthzErrors) throws TException {
      try {
        return tableLoaders.get(TableName.table(dbName,tableName));
      } catch (final ExecutionException e) {
        throw new TException(e);
      }
    }

    @Override
    public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws TException {
      synchronized (this) {
        return super.getDelegationToken(owner, renewerKerberosPrincipalName);
      }
    }

  }

  private class DatabaseLoader extends CacheLoader<String, List<String>> {
    @Override
    public List<String> load(String key) throws Exception {
      if (!"databases".equals(key)) {
        throw new UnsupportedOperationException();
      }
      synchronized (DrillHiveMetaStoreClient.this) {
        return getDatabasesHelper(DrillHiveMetaStoreClient.this);
      }
    }
  }

  private class TableNameLoader extends CacheLoader<String, List<String>> {
    @Override
    public List<String> load(String dbName) throws Exception {
      synchronized (DrillHiveMetaStoreClient.this) {
        return getTableNamesHelper(DrillHiveMetaStoreClient.this, dbName);
      }
    }
  }

  private class TableLoader extends CacheLoader<TableName, HiveReadEntry> {
    @Override
    public HiveReadEntry load(TableName key) throws Exception {
      synchronized (DrillHiveMetaStoreClient.this) {
        return getHiveReadEntryHelper(DrillHiveMetaStoreClient.this, key.getDatabaseName(), key.getTableName());
      }
    }
  }

  static class TableName {
    private final String databaseName;
    private final String tableName;

    private TableName(String databaseName, String tableName) {
      this.databaseName = databaseName;
      this.tableName = tableName;
    }

    public static TableName table(String databaseName, String tableName) {
      return new TableName(databaseName, tableName);
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public String getTableName() {
      return tableName;
    }

    @Override
    public String toString() {
      return String.format("databaseName:%s, tableName:%s", databaseName, tableName).toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TableName other = (TableName) o;
      return Objects.equals(databaseName, other.databaseName) &&
          Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(databaseName, tableName);
    }
  }

}
