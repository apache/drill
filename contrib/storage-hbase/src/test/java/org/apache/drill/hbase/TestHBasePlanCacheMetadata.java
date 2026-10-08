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

import java.io.IOException;

import com.google.common.hash.Hashing;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.PlanCacheTable;
import org.apache.drill.exec.store.hbase.HBaseScanSpec;
import org.apache.drill.exec.store.hbase.HBaseStoragePlugin;
import org.apache.drill.exec.store.hbase.HBaseStoragePluginConfig;
import org.apache.drill.test.BaseTest;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestHBasePlanCacheMetadata extends BaseTest {
  private static final TableName NAME = TableName.valueOf("plan_cache_metadata");
  private final HBaseScanSpec selection = new HBaseScanSpec(NAME.getNameAsString());
  private Connection connection;
  private Table table;
  private HBaseStoragePlugin plugin;

  @Before
  public void setup() throws Exception {
    connection = mock(Connection.class);
    table = mock(Table.class);
    when(connection.getTable(NAME)).thenReturn(table);
    plugin = spy(new HBaseStoragePlugin(new HBaseStoragePluginConfig(null, false),
        mock(DrillbitContext.class), "hbase"));
    doReturn(connection).when(plugin).getConnection();
  }

  @Test
  public void testDescriptorFingerprintPreservesExistingCompatibilityVersion() throws Exception {
    TableDescriptor descriptor = descriptor("cf1");
    when(table.getDescriptor()).thenReturn(descriptor);
    PlanCacheTable metadata = plugin.planCacheTable(selection);
    assertEquals(NAME.getNameAsString(), metadata.getIdentifier());
    assertEquals(Hashing.sha256().hashBytes(new HTableDescriptor(descriptor).toByteArray()).toString(),
        metadata.getVersion());
    verify(table).getDescriptor();
    verify(table).close();
    verifyNoMoreInteractions(table);
    verify(connection).getTable(NAME);
    // No Admin handle, separate existence check or connection close is needed.
    verifyNoMoreInteractions(connection);
  }

  @Test
  public void testSchemaChangeChangesCompatibilityVersion() throws Exception {
    when(table.getDescriptor()).thenReturn(descriptor("cf1"), descriptor("cf2"));
    assertNotEquals(plugin.planCacheTable(selection).getVersion(),
        plugin.planCacheTable(selection).getVersion());
  }

  @Test
  public void testMissingDescriptorReturnsNoMetadataAndClosesTable() throws Exception {
    when(table.getDescriptor()).thenThrow(new TableNotFoundException(NAME));
    assertNull(plugin.planCacheTable(selection));
    verify(table).close();
  }

  @Test
  public void testMissingTableHandleReturnsNoMetadata() throws Exception {
    when(connection.getTable(NAME)).thenThrow(new TableNotFoundException(NAME));
    assertNull(plugin.planCacheTable(selection));
    verifyNoInteractions(table);
  }

  @Test
  public void testOtherIoFailuresPropagateAndCloseTable() throws Exception {
    IOException failure = new IOException("descriptor read failed");
    when(table.getDescriptor()).thenThrow(failure);
    assertSame(failure, assertThrows(IOException.class, () -> plugin.planCacheTable(selection)));
    verify(table).close();
  }

  private TableDescriptor descriptor(String family) {
    return TableDescriptorBuilder.newBuilder(NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(family))).build();
  }
}
