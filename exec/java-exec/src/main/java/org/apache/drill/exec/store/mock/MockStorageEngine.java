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
package org.apache.drill.exec.store.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.mock.MockGroupScanPOP.MockScanEntry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public class MockStorageEngine extends AbstractStoragePlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorageEngine.class);

  private final MockStorageEngineConfig configuration;
  private final MockSchema schema;

  public MockStorageEngine(MockStorageEngineConfig configuration, DrillbitContext context, String name) {
    this.configuration = configuration;
    this.schema = new MockSchema(this);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns)
      throws IOException {

    List<MockScanEntry> readEntries = selection.getListWith(new ObjectMapper(),
        new TypeReference<ArrayList<MockScanEntry>>() {
        });

    // The classic (logical-plan based) and extended (SQL-based) paths
    // come through here. If this is a SQL query, then no columns are
    // defined in the plan.

    assert ! readEntries.isEmpty();
    boolean extended = readEntries.size() == 1;
    if (extended) {
      MockScanEntry entry = readEntries.get(0);
      extended = entry.getTypes() == null;
    }
    return new MockGroupScanPOP(null, extended, readEntries);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    parent.add(schema.getName(), schema);
  }

  @Override
  public StoragePluginConfig getConfig() {
    return configuration;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

//  public static class ImplicitTable extends DynamicDrillTable {
//
//    public ImplicitTable(StoragePlugin plugin, String storageEngineName,
//        Object selection) {
//      super(plugin, storageEngineName, selection);
//    }
//
//  }

  private static class MockSchema extends AbstractSchema {

    private MockStorageEngine engine;

    public MockSchema(MockStorageEngine engine) {
      super(ImmutableList.<String>of(), MockStorageEngineConfig.NAME);
      this.engine = engine;
    }

    @Override
    public Table getTable(String name) {
      Pattern p = Pattern.compile("(\\w+)_(\\d+)(k|m)?", Pattern.CASE_INSENSITIVE);
      Matcher m = p.matcher(name);
      if (! m.matches()) {
        return null;
      }
      @SuppressWarnings("unused")
      String baseName = m.group(1);
      int n = Integer.parseInt(m.group(2));
      String unit = m.group(3);
      if (unit.equalsIgnoreCase("K")) { n *= 1000; }
      else if (unit.equalsIgnoreCase("M")) { n *= 1_000_000; }
      MockScanEntry entry = new MockScanEntry(n, null);
      List<MockScanEntry> list = new ArrayList<>();
      list.add(entry);
      return new DynamicDrillTable(engine, this.name, list);
    }

    @Override
    public Set<String> getTableNames() {
      return new HashSet<>();
    }

    @Override
    public String getTypeName() {
      return MockStorageEngineConfig.NAME;
    }
  }
}
