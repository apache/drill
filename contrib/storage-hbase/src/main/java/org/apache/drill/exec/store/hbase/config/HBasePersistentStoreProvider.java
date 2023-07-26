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
package org.apache.drill.exec.store.hbase.config;

import java.io.IOException;
import java.util.Map;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.store.hbase.DrillHBaseConstants;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreRegistry;
import org.apache.drill.exec.store.sys.store.provider.BasePersistentStoreProvider;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePersistentStoreProvider extends BasePersistentStoreProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBasePersistentStoreProvider.class);

  public static final byte[] DEFAULT_FAMILY_NAME = Bytes.toBytes("s");

  public static final byte[] QUALIFIER_NAME = Bytes.toBytes("d");

  private static final String HBASE_CLIENT_ID = "drill-hbase-persistent-store-client";

  private final TableName hbaseTableName;

  private final byte[] family;

  private Table hbaseTable;

  private Configuration hbaseConf;

  private final Map<String, Object> tableConfig;

  private final Map<String, Object> columnConfig;

  private Connection connection;

  @SuppressWarnings("unchecked")
  public HBasePersistentStoreProvider(PersistentStoreRegistry registry) {
    final Map<String, Object> hbaseConfig = (Map<String, Object>) registry.getConfig().getAnyRef(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_CONFIG);
    if (registry.getConfig().hasPath(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_TABLE_CONFIG)) {
      tableConfig = (Map<String, Object>) registry.getConfig().getAnyRef(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_TABLE_CONFIG);
    } else {
      tableConfig = Maps.newHashMap();
    }
    if (registry.getConfig().hasPath(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_COLUMN_CONFIG)) {
      columnConfig = (Map<String, Object>) registry.getConfig().getAnyRef(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_COLUMN_CONFIG);
    } else {
      columnConfig = Maps.newHashMap();
    }
    hbaseConf = HBaseConfiguration.create();
    hbaseConf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, HBASE_CLIENT_ID);
    if (hbaseConfig != null) {
      for (Map.Entry<String, Object> entry : hbaseConfig.entrySet()) {
        hbaseConf.set(entry.getKey(), String.valueOf(entry.getValue()));
      }
    }
    logger.info("Received the hbase config is {}", hbaseConfig);
    if (!tableConfig.isEmpty()) {
      logger.info("Received the table config is {}", tableConfig);
    }
    if (!columnConfig.isEmpty()) {
      logger.info("Received the column config is {}", columnConfig);
    }
    String tableName = registry.getConfig().getString(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_TABLE);
    if (registry.getConfig().hasPath(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_NAMESPACE)) {
      String namespaceStr = registry.getConfig().getString(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_NAMESPACE);
      hbaseTableName = TableName.valueOf(namespaceStr.concat(":").concat(tableName));
    } else {
      hbaseTableName = TableName.valueOf(tableName);
    }
    if (registry.getConfig().hasPath(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_FAMILY)) {
      String familyStr = registry.getConfig().getString(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_FAMILY);
      family = Bytes.toBytes(familyStr);
    } else { // The default name
      family = DEFAULT_FAMILY_NAME;
    }
  }

  @VisibleForTesting
  public HBasePersistentStoreProvider(Configuration conf, String storeTableName) {
    this.tableConfig = Maps.newHashMap();
    this.columnConfig = Maps.newHashMap();
    this.hbaseConf = conf;
    this.hbaseTableName = TableName.valueOf(storeTableName);
    this.family = DEFAULT_FAMILY_NAME;
  }

  @VisibleForTesting
  public HBasePersistentStoreProvider(Map<String, Object> tableConfig, Map<String, Object> columnConfig, Configuration conf, String storeTableName) {
    this.tableConfig = tableConfig;
    this.columnConfig = columnConfig;
    this.hbaseConf = conf;
    this.hbaseTableName = TableName.valueOf(storeTableName);
    this.family = DEFAULT_FAMILY_NAME;
  }

  @Override
  public <V> PersistentStore<V> getOrCreateStore(PersistentStoreConfig<V> config) throws StoreException {
    switch (config.getMode()) {
    case BLOB_PERSISTENT:
    case PERSISTENT:
      return new HBasePersistentStore<>(config, hbaseTable, family);
    default:
      throw new IllegalStateException("Unknown persistent mode");
    }
  }

  @Override
  public void start() throws IOException {
    // Create the column family builder
    ColumnFamilyDescriptorBuilder columnFamilyBuilder = ColumnFamilyDescriptorBuilder
        .newBuilder(family)
        .setMaxVersions(1);
    // Append the config to column family
    verifyAndSetColumnConfig(columnConfig, columnFamilyBuilder);
    // Create the table builder
    TableDescriptorBuilder tableBuilder = TableDescriptorBuilder
        .newBuilder(hbaseTableName)
        .setColumnFamily(columnFamilyBuilder.build());
    // Append the config to table
    verifyAndSetTableConfig(tableConfig, tableBuilder);
    this.connection = ConnectionFactory.createConnection(hbaseConf);
    try(Admin admin = connection.getAdmin()) {
      if (!admin.tableExists(hbaseTableName)) {
        // Go to create the table
        admin.createTable(tableBuilder.build());
        logger.info("The HBase table of persistent store created : {}", hbaseTableName);
      } else {
        TableDescriptor table = admin.getDescriptor(hbaseTableName);
        if (!admin.isTableEnabled(hbaseTableName)) {
          admin.enableTable(hbaseTableName); // In case the table is disabled
        }
        if (!table.hasColumnFamily(family)) {
          throw new DrillRuntimeException("The HBase table " + hbaseTableName
              + " specified as persistent store exists but does not contain column family: "
              + (Bytes.toString(family)));
        }
        logger.info("The HBase table of persistent store is loaded : {}", hbaseTableName);
      }
    }

    this.hbaseTable = connection.getTable(hbaseTableName);
  }

  /**
   * Verify the configuration of HBase table and
   * add them to the table builder.
   * @param config  Received the table config
   * @param builder HBase table builder
   */
  private void verifyAndSetTableConfig(Map<String, Object> config, TableDescriptorBuilder builder) {
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      switch (entry.getKey().toUpperCase()) {
      case TableDescriptorBuilder.DURABILITY:
        Durability durability = Durability.valueOf(((String) entry.getValue()).toUpperCase());
        builder.setDurability(durability);
        break;
      case TableDescriptorBuilder.COMPACTION_ENABLED:
        builder.setCompactionEnabled((Boolean) entry.getValue());
        break;
      case TableDescriptorBuilder.SPLIT_ENABLED:
        builder.setSplitEnabled((Boolean) entry.getValue());
        break;
      case TableDescriptorBuilder.FLUSH_POLICY:
        builder.setFlushPolicyClassName((String) entry.getValue());
        break;
      case TableDescriptorBuilder.SPLIT_POLICY:
        builder.setRegionSplitPolicyClassName((String) entry.getValue());
        break;
      case TableDescriptorBuilder.MAX_FILESIZE:
        builder.setMaxFileSize((Integer) entry.getValue());
        break;
      case TableDescriptorBuilder.MEMSTORE_FLUSHSIZE:
        builder.setMemStoreFlushSize((Integer) entry.getValue());
        break;
      default:
        break;
      }
    }
  }

  /**
   * Verify the configuration of HBase column family and
   * add them to the column family builder.
   * @param config  Received the column config
   * @param builder HBase column family builder
   */
  private void verifyAndSetColumnConfig(Map<String, Object> config, ColumnFamilyDescriptorBuilder builder) {
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      switch (entry.getKey().toUpperCase()) {
      case ColumnFamilyDescriptorBuilder.MAX_VERSIONS:
        builder.setMaxVersions((Integer) entry.getValue());
        break;
      case ColumnFamilyDescriptorBuilder.TTL:
        builder.setTimeToLive((Integer) entry.getValue());
        break;
      case ColumnFamilyDescriptorBuilder.COMPRESSION:
        Algorithm algorithm = Algorithm.valueOf(((String) entry.getValue()).toUpperCase());
        builder.setCompressionType(algorithm);
        break;
      case ColumnFamilyDescriptorBuilder.BLOCKCACHE:
        builder.setBlockCacheEnabled((Boolean) entry.getValue());
        break;
      case ColumnFamilyDescriptorBuilder.BLOCKSIZE:
        builder.setBlocksize((Integer) entry.getValue());
        break;
      case ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING:
        DataBlockEncoding encoding = DataBlockEncoding.valueOf(((String) entry.getValue()).toUpperCase());
        builder.setDataBlockEncoding(encoding);
        break;
      case ColumnFamilyDescriptorBuilder.IN_MEMORY:
        builder.setInMemory((Boolean) entry.getValue());
        break;
      case ColumnFamilyDescriptorBuilder.DFS_REPLICATION:
        builder.setDFSReplication(((Integer) entry.getValue()).shortValue());
        break;
      default:
        break;
      }
    }
  }

  @Override
  public synchronized void close() {
    if (hbaseTable != null) {
      AutoCloseables.closeSilently(hbaseTable);
    }
    if (connection != null && !connection.isClosed()) {
      AutoCloseables.closeSilently(connection);
    }
    logger.info("The HBase connection of persistent store closed.");
  }
}
