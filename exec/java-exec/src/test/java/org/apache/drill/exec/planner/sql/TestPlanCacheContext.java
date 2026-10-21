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
package org.apache.drill.exec.planner.sql;

import java.time.Duration;
import java.util.Collections;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.drill.categories.PlannerTest;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTableSelection;
import org.apache.drill.exec.server.options.QueryOptionManager;
import org.apache.drill.exec.store.PlanCacheTable;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.BaseTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(PlannerTest.class)
public class TestPlanCacheContext extends BaseTest {
  private QueryContext context;
  private SchemaPlus schema;
  private StoragePluginRegistry registry;
  private StoragePlugin pluginA;
  private StoragePluginConfig configA;
  private DrillTableSelection selectionA;

  @Before
  public void setup() throws Exception {
    context = mock(QueryContext.class);
    schema = mock(SchemaPlus.class);
    registry = mock(StoragePluginRegistry.class);
    QueryOptionManager options = mock(QueryOptionManager.class);
    when(options.iterator()).thenAnswer(call -> Collections.emptyIterator());
    when(context.getOptions()).thenReturn(options);
    when(context.getStorage()).thenReturn(registry);
    configA = mock(StoragePluginConfig.class);
    pluginA = mock(StoragePlugin.class);
    selectionA = mock(DrillTableSelection.class);
    addTable("a", pluginA, configA, selectionA);
    addTable("b", mock(StoragePlugin.class), mock(StoragePluginConfig.class), mock(DrillTableSelection.class));
  }

  @Test
  public void testPluginConfigChangesPartitionCacheKey() throws Exception {
    PlanCache.ContextSnapshot original = snapshot("SELECT * FROM \"a\"");
    when(registry.encode(configA)).thenReturn("changed plugin configuration");
    PlanCache.ContextSnapshot changed = snapshot("SELECT * FROM \"a\"");
    assertNotEquals(original.keyFingerprint(), changed.keyFingerprint());
  }

  @Test
  public void testPluginTraversalOrderDoesNotChangeFingerprint() throws Exception {
    PlanCache.ContextSnapshot first = snapshot("SELECT * FROM \"a\" JOIN \"b\" ON TRUE");
    PlanCache.ContextSnapshot reversed = snapshot("SELECT * FROM \"b\" JOIN \"a\" ON TRUE");
    assertEquals(first.keyFingerprint(), reversed.keyFingerprint());
  }

  @Test
  public void testTableVersionChangeInvalidatesEntryWithoutChangingKey() throws Exception {
    PlanCache.ContextSnapshot original = snapshot("SELECT * FROM \"a\"");
    when(pluginA.planCacheTable(selectionA)).thenReturn(new PlanCacheTable("a", "version-2"));
    PlanCache.ContextSnapshot changed = snapshot("SELECT * FROM \"a\"");
    assertEquals(original.keyFingerprint(), changed.keyFingerprint());
    try (PlanCache cache = new PlanCache(1024, Duration.ZERO, Duration.ZERO)) {
      assertTrue(cache.put("key", "{}", null, mock(PhysicalPlanReader.class), original));
      assertTrue(cache.get("key").matchesContext(original));
      assertFalse(cache.get("key").matchesContext(changed));
    }
  }

  private void addTable(String name, StoragePlugin plugin, StoragePluginConfig config,
      DrillTableSelection selection) throws Exception {
    DrillTable table = mock(DrillTable.class);
    when(table.getSelection()).thenReturn(selection);
    when(table.getPlugin()).thenReturn(plugin);
    when(table.getStorageEngineName()).thenReturn("storage-" + name);
    when(schema.getTable(name)).thenReturn(table);
    when(plugin.supportPlanCache(selection)).thenReturn(true);
    when(plugin.planCacheTable(selection)).thenReturn(new PlanCacheTable(name, "version-1"));
    when(plugin.getConfig()).thenReturn(config);
    when(registry.encode(config)).thenReturn("configuration-" + name);
  }

  private PlanCache.ContextSnapshot snapshot(String sql) throws Exception {
    PlanCache.ContextSnapshot snapshot = PlanCache.ContextSnapshot.resolve(
        schema, SqlParser.create(sql).parseQuery(), context);
    assertNotNull(snapshot);
    return snapshot;
  }
}
