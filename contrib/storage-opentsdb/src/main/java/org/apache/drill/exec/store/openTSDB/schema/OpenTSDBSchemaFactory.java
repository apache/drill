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
package org.apache.drill.exec.store.openTSDB.schema;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.openTSDB.DrillOpenTSDBTable;
import org.apache.drill.exec.store.openTSDB.OpenTSDBScanSpec;
import org.apache.drill.exec.store.openTSDB.OpenTSDBStoragePlugin;
import org.apache.drill.exec.store.openTSDB.OpenTSDBStoragePluginConfig;
import org.apache.drill.exec.store.openTSDB.client.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class OpenTSDBSchemaFactory implements SchemaFactory {

  private static final Logger log = LoggerFactory.getLogger(OpenTSDBSchemaFactory.class);

  private final String schemaName;
  private final OpenTSDBStoragePlugin plugin;

  public OpenTSDBSchemaFactory(OpenTSDBStoragePlugin plugin, String schemaName) {
    this.plugin = plugin;
    this.schemaName = schemaName;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    OpenTSDBSchema schema = new OpenTSDBSchema(schemaName);
    parent.add(schemaName, schema);
  }

  class OpenTSDBSchema extends AbstractSchema {

    OpenTSDBSchema(String name) {
      super(Collections.<String>emptyList(), name);
    }

    @Override
    public Table getTable(String name) {
      OpenTSDBScanSpec scanSpec = new OpenTSDBScanSpec(name);
      try {
        return new DrillOpenTSDBTable(schemaName, plugin, new Schema(plugin.getClient(), name), scanSpec);
      } catch (Exception e) {
        // Calcite firstly looks for a table in the default schema, if the table was not found,
        // it looks in the root schema.
        // If the table does not exist, a query will fail at validation stage,
        // so the error should not be thrown here.
        logger.warn("Failure while loading table '{}' for database '{}'.", name, schemaName, e.getCause());
        return null;
      }
    }

    @Override
    public Set<String> getTableNames() {
      return plugin.getClient().getAllMetricNames();
    }

    @Override
    public String getTypeName() {
      return OpenTSDBStoragePluginConfig.NAME;
    }
  }
}
