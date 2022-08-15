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

package org.apache.drill.exec.store.splunk;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SplunkSchemaFactory extends AbstractSchemaFactory {

  private static final Logger logger = LoggerFactory.getLogger(SplunkSchemaFactory.class);
  private static final String SPL_TABLE_NAME = "spl";
  private final SplunkStoragePlugin plugin;
  private String queryUserName;

  public SplunkSchemaFactory(SplunkStoragePlugin plugin) {
    super(plugin.getName());
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    this.queryUserName = schemaConfig.getUserName();
    SplunkSchema schema = new SplunkSchema(plugin, queryUserName);
    SchemaPlus plusOfThis = parent.add(schema.getName(), schema);
  }

  static class SplunkSchema extends AbstractSchema {

    private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();
    private final SplunkStoragePlugin plugin;
    private final String queryUserName;

    public SplunkSchema(SplunkStoragePlugin plugin, String queryUserName) {
      super(Collections.emptyList(), plugin.getName());
      this.plugin = plugin;
      this.queryUserName = queryUserName;


      registerIndexes();
    }

    @Override
    public Table getTable(String name) {
      DynamicDrillTable table = activeTables.get(name);
      if (table != null) {
        // If the table was found, return it.
        return table;
      } else {
        // Register the table
        return registerTable(name, new DynamicDrillTable(plugin, plugin.getName(),
          new SplunkScanSpec(plugin.getName(), name, plugin.getConfig(), queryUserName)));
      }
    }

    @Override
    public boolean showInInformationSchema() {
      return true;
    }

    @Override
    public Set<String> getTableNames() {
      return Sets.newHashSet(activeTables.keySet());
    }

    private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
      activeTables.put(name, table);
      return table;
    }

    @Override
    public String getTypeName() {
      return SplunkPluginConfig.NAME;
    }

    private void registerIndexes() {
      // Verify that the connection is successful.  If not, don't register any indexes,
      // and throw an exception.
      SplunkPluginConfig config = plugin.getConfig();
      SplunkConnection connection;
      try {
        connection = new SplunkConnection(config, queryUserName);
        connection.connect();
      } catch (Exception e) {
        // Catch any connection errors that may happen.
        throw UserException.connectionError()
          .message("Unable to connect to Splunk: " +  plugin.getName() + " " + e.getMessage())
          .build(logger);
      }

      // Add default "spl" table to index list.
      registerTable(SPL_TABLE_NAME, new DynamicDrillTable(plugin, plugin.getName(),
        new SplunkScanSpec(plugin.getName(), SPL_TABLE_NAME, plugin.getConfig(), queryUserName)));

      // Retrieve and add all other Splunk indexes
      for (String indexName : connection.getIndexes().keySet()) {
        logger.debug("Registering {}", indexName);
        registerTable(indexName, new DynamicDrillTable(plugin, plugin.getName(),
          new SplunkScanSpec(plugin.getName(), indexName, config, queryUserName)));
      }
    }
  }
}
