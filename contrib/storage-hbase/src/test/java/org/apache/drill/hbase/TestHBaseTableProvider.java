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
package org.apache.drill.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.categories.HBaseStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.store.hbase.config.HBasePersistentStoreProvider;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SlowTest.class, HBaseStorageTest.class})
public class TestHBaseTableProvider extends BaseHBaseTest {

  private static HBasePersistentStoreProvider provider;

  private static final String STORE_TABLENAME = "drill_store";

  private static final int MAX_FILESIZE = 1073741824;
  private static final int MEMSTORE_FLUSHSIZE = 536870912;
  private static final int MAX_VERSIONS = 1;
  private static final int TTL = 3600;
  private static final int BLOCKSIZE = 102400;
  private static final int DFS_REPLICATION = 1;

  private static final Map<String, Object> tableConfig;
  private static final Map<String, Object> columnConfig;

  static {
    // Table level
    tableConfig = new HashMap<>();
    tableConfig.put(TableDescriptorBuilder.DURABILITY, Durability.ASYNC_WAL.name());
    tableConfig.put(TableDescriptorBuilder.COMPACTION_ENABLED, false);
    tableConfig.put(TableDescriptorBuilder.SPLIT_ENABLED, false);
    tableConfig.put(TableDescriptorBuilder.MAX_FILESIZE, MAX_FILESIZE); // 1GB
    tableConfig.put(TableDescriptorBuilder.MEMSTORE_FLUSHSIZE, MEMSTORE_FLUSHSIZE); // 512 MB
    // Column Family level
    columnConfig = new HashMap<>();
    columnConfig.put(ColumnFamilyDescriptorBuilder.MAX_VERSIONS, MAX_VERSIONS);
    columnConfig.put(ColumnFamilyDescriptorBuilder.TTL, TTL); // 1 HOUR
    columnConfig.put(ColumnFamilyDescriptorBuilder.COMPRESSION, Compression.Algorithm.NONE.name());
    columnConfig.put(ColumnFamilyDescriptorBuilder.BLOCKCACHE, false);
    columnConfig.put(ColumnFamilyDescriptorBuilder.BLOCKSIZE, BLOCKSIZE);
    columnConfig.put(ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING, DataBlockEncoding.FAST_DIFF.name());
    columnConfig.put(ColumnFamilyDescriptorBuilder.IN_MEMORY, true);
    columnConfig.put(ColumnFamilyDescriptorBuilder.DFS_REPLICATION, DFS_REPLICATION);
  }

  @BeforeClass // mask HBase cluster start function
  public static void setUpBeforeTestHBaseTableProvider() throws Exception {
    provider = new HBasePersistentStoreProvider(tableConfig, columnConfig, storagePluginConfig.getHBaseConf(), STORE_TABLENAME);
    provider.start();
  }

  @Test
  public void testStoreTableAttributes() throws Exception {
    TableName tableName = TableName.valueOf(STORE_TABLENAME);
    try(Admin tableAdmin = HBaseTestsSuite.conn.getAdmin()) {
      assertTrue("The store table not found : " + STORE_TABLENAME, tableAdmin.tableExists(tableName));
      // Table verify
      TableDescriptor tableDescriptor = tableAdmin.getDescriptor(tableName);
      assertTrue("The durability must be " + Durability.ASYNC_WAL, tableDescriptor.getDurability() == Durability.ASYNC_WAL);
      assertTrue("The compaction must be disabled", !tableDescriptor.isCompactionEnabled());
      assertTrue("The split must be disabled", !tableDescriptor.isSplitEnabled());
      assertTrue("The max size of hfile must be " + MAX_FILESIZE, tableDescriptor.getMaxFileSize() == MAX_FILESIZE);
      assertTrue("The memstore size must be " + MEMSTORE_FLUSHSIZE, tableDescriptor.getMemStoreFlushSize() == MEMSTORE_FLUSHSIZE);
      // Column Family verify
      assertTrue("The column family not found", tableDescriptor.hasColumnFamily(HBasePersistentStoreProvider.DEFAULT_FAMILY_NAME));
      ColumnFamilyDescriptor columnDescriptor = tableDescriptor.getColumnFamily(HBasePersistentStoreProvider.DEFAULT_FAMILY_NAME);
      assertTrue("The max number of versions must be " + MAX_VERSIONS, columnDescriptor.getMaxVersions() == MAX_VERSIONS);
      assertTrue("The time-to-live must be " + TTL, columnDescriptor.getTimeToLive() == TTL);
      // TODO native snappy* library not available
      assertTrue("The algorithm of compression must be " + Algorithm.NONE, columnDescriptor.getCompressionType() == Algorithm.NONE);
      assertTrue("The block cache must be disabled", columnDescriptor.isBlockCacheEnabled() == false);
      assertTrue("The block size must be " + BLOCKSIZE, columnDescriptor.getBlocksize() == BLOCKSIZE);
      assertTrue("The encoding of data block must be " + DataBlockEncoding.FAST_DIFF, columnDescriptor.getDataBlockEncoding() == DataBlockEncoding.FAST_DIFF);
      assertTrue("The in-memory must be enabled", columnDescriptor.isInMemory());
      assertTrue("The replication of dfs must be " + DFS_REPLICATION, columnDescriptor.getDFSReplication() == DFS_REPLICATION);
    }
  }

  @Test
  public void testTableProvider() throws StoreException {
    LogicalPlanPersistence lp = PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(config);
    {
      PersistentStoreConfig<String> storeConfig = PersistentStoreConfig.newJacksonBuilder(lp.getMapper(), String.class).name("hbase").build();
      PersistentStore<String> hbaseStore = provider.getOrCreateStore(storeConfig);
      hbaseStore.put("", "v0");
      hbaseStore.put("k1", "v1");
      hbaseStore.put("k2", "v2");
      hbaseStore.put("k3", "v3");
      hbaseStore.put("k4", "v4");
      hbaseStore.put("k5", "v5");
      hbaseStore.put(".test", "testValue");

      assertEquals("v0", hbaseStore.get(""));
      assertEquals("testValue", hbaseStore.get(".test"));

      assertTrue(hbaseStore.contains(""));
      assertFalse(hbaseStore.contains("unknown_key"));

      assertEquals(7, Lists.newArrayList(hbaseStore.getAll()).size());
    }

    {
      PersistentStoreConfig<String> storeConfig = PersistentStoreConfig.newJacksonBuilder(lp.getMapper(), String.class).name("hbase.test").build();
      PersistentStore<String> hbaseStore = provider.getOrCreateStore(storeConfig);
      hbaseStore.put("", "v0");
      hbaseStore.put("k1", "v1");
      hbaseStore.put("k2", "v2");
      hbaseStore.put("k3", "v3");
      hbaseStore.put("k4", "v4");
      hbaseStore.put(".test", "testValue");

      assertEquals("v0", hbaseStore.get(""));
      assertEquals("testValue", hbaseStore.get(".test"));

      assertEquals(6, Lists.newArrayList(hbaseStore.getAll()).size());
    }
  }

  @AfterClass
  public static void tearDownTestHBaseTableProvider() {
    if (provider != null) {
      provider.close();
    }
  }
}
