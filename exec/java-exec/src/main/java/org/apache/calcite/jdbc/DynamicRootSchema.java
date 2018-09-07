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
package org.apache.calcite.jdbc;

import org.apache.calcite.DataContext;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.SubSchemaWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is to allow us loading schemas from storage plugins later when {@link #getSubSchema(String, boolean)}
 * is called.
 */
public class DynamicRootSchema extends DynamicSchema {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamicRootSchema.class);

  protected SchemaConfig schemaConfig;
  protected StoragePluginRegistry storages;

  public StoragePluginRegistry getSchemaFactories() {
    return storages;
  }

  /** Creates a root schema. */
  DynamicRootSchema(StoragePluginRegistry storages, SchemaConfig schemaConfig) {
    super(null, new RootSchema(), "");
    this.schemaConfig = schemaConfig;
    this.storages = storages;
  }

  @Override
  protected CalciteSchema getImplicitSubSchema(String schemaName,
                                               boolean caseSensitive) {
    CalciteSchema retSchema = getSubSchemaMap().get(schemaName);
    if (retSchema != null) {
      return retSchema;
    }

    loadSchemaFactory(schemaName, caseSensitive);
    retSchema = getSubSchemaMap().get(schemaName);
    return retSchema;
  }

  /**
   * Loads schema factory(storage plugin) for specified {@code schemaName}
   * @param schemaName the name of the schema
   * @param caseSensitive whether matching for the schema name is case sensitive
   */
  public void loadSchemaFactory(String schemaName, boolean caseSensitive) {
    try {
      SchemaPlus schemaPlus = this.plus();
      StoragePlugin plugin = getSchemaFactories().getPlugin(schemaName);
      if (plugin != null && plugin.getConfig().isEnabled()) {
        plugin.registerSchemas(schemaConfig, schemaPlus);
        return;
      }

      // Could not find the plugin of schemaName. The schemaName could be `dfs.tmp`, a 2nd level schema under 'dfs'
      List<String> paths = SchemaUtilites.getSchemaPathAsList(schemaName);
      if (paths.size() == 2) {
        plugin = getSchemaFactories().getPlugin(paths.get(0));
        if (plugin == null) {
          return;
        }

        // Looking for the SchemaPlus for the top level (e.g. 'dfs') of schemaName (e.g. 'dfs.tmp')
        SchemaPlus firstLevelSchema = schemaPlus.getSubSchema(paths.get(0));
        if (firstLevelSchema == null) {
          // register schema for this storage plugin to 'this'.
          plugin.registerSchemas(schemaConfig, schemaPlus);
          firstLevelSchema = schemaPlus.getSubSchema(paths.get(0));
        }
        // Load second level schemas for this storage plugin
        List<SchemaPlus> secondLevelSchemas = new ArrayList<>();
        for (String secondLevelSchemaName : firstLevelSchema.getSubSchemaNames()) {
          secondLevelSchemas.add(firstLevelSchema.getSubSchema(secondLevelSchemaName));
        }

        for (SchemaPlus schema : secondLevelSchemas) {
          org.apache.drill.exec.store.AbstractSchema drillSchema;
          try {
            drillSchema = schema.unwrap(org.apache.drill.exec.store.AbstractSchema.class);
          } catch (ClassCastException e) {
            throw new RuntimeException(String.format("Schema '%s' is not expected under root schema", schema.getName()));
          }
          SubSchemaWrapper wrapper = new SubSchemaWrapper(drillSchema);
          schemaPlus.add(wrapper.getName(), wrapper);
        }
      }
    } catch(ExecutionSetupException | IOException ex) {
      logger.warn("Failed to load schema for \"" + schemaName + "\"!", ex);
    }
  }

  static class RootSchema extends AbstractSchema {
    @Override public Expression getExpression(SchemaPlus parentSchema,
                                              String name) {
      return Expressions.call(
          DataContext.ROOT,
          BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
    }
  }
}

