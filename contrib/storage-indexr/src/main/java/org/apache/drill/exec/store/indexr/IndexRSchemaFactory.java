/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import io.indexr.segment.SegmentSchema;
import io.indexr.server.HybridTable;

public class IndexRSchemaFactory implements SchemaFactory {
  private static final Logger log = LoggerFactory.getLogger(IndexRSchemaFactory.class);

  private final IndexRStoragePlugin plugin;

  public IndexRSchemaFactory(IndexRStoragePlugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    parent.add(plugin.pluginName(), new IndexRTables(plugin.pluginName()));
  }

  class IndexRTables extends AbstractSchema {

    public IndexRTables(String name) {
      super(Collections.<String>emptyList(), name);
    }

    @Override
    public String getTypeName() {
      return IndexRStoragePluginConfig.NAME;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return Collections.emptySet();
    }

    @Override
    public Table getTable(String name) {
      IndexRScanSpec spec = new IndexRScanSpec(name);
      HybridTable table = plugin.indexRNode().getTablePool().get(name);
      if (table == null) {
        return null;
      }
      SegmentSchema schema = table.schema().schema;
      return new DrillIndexRTable(plugin, spec, schema);
    }

    @Override
    public Set<String> getTableNames() {
      try {
        return plugin.indexRNode().getTablePool().allTableNames();
      } catch (Exception e) {
        log.warn("", e);
      }
      return Collections.emptySet();
    }
  }
}
