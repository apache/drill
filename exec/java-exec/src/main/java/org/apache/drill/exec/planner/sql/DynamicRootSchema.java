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

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteRootSchema;
import org.apache.calcite.jdbc.CalciteSchema;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Compatible;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.SubSchemaWrapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

/**
 * This class is to allow us loading schemas from storage plugins later when {@link #getSubSchema(String, boolean)}
 * is called.
 */
public class DynamicRootSchema extends DynamicSchema
    implements CalciteRootSchema {
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
  public CalciteSchema getSubSchema(String schemaName, boolean caseSensitive) {
    CalciteSchema retSchema = getSubSchemaMap().get(schemaName);
    if (retSchema != null) {
      return retSchema;
    }

    loadSchemaFactory(schemaName, caseSensitive);
    retSchema = getSubSchemaMap().get(schemaName);
    return retSchema;
  }

  @Override
  public NavigableSet<String> getTableNames() {
    return Compatible.INSTANCE.navigableSet(ImmutableSortedSet.<String>of());
  }

  /**
   * load schema factory(storage plugin) for schemaName
   * @param schemaName
   * @param caseSensitive
   */
  public void loadSchemaFactory(String schemaName, boolean caseSensitive) {
    try {
      SchemaPlus thisPlus = this.plus();
      StoragePlugin plugin = getSchemaFactories().getPlugin(schemaName);
      if (plugin != null) {
        plugin.registerSchemas(schemaConfig, thisPlus);
        return;
      }

      // Could not find the plugin of schemaName. The schemaName could be `dfs.tmp`, a 2nd level schema under 'dfs'
      String[] paths = schemaName.split("\\.");
      if (paths.length == 2) {
        plugin = getSchemaFactories().getPlugin(paths[0]);
        if (plugin == null) {
          return;
        }

        // Found the storage plugin for first part(e.g. 'dfs') of schemaName (e.g. 'dfs.tmp')
        // register schema for this storage plugin to 'this'.
        plugin.registerSchemas(schemaConfig, thisPlus);

        // Load second level schemas for this storage plugin
        final SchemaPlus firstlevelSchema = thisPlus.getSubSchema(paths[0]);
        final List<SchemaPlus> secondLevelSchemas = Lists.newArrayList();
        for (String secondLevelSchemaName : firstlevelSchema.getSubSchemaNames()) {
          secondLevelSchemas.add(firstlevelSchema.getSubSchema(secondLevelSchemaName));
        }

        for (SchemaPlus schema : secondLevelSchemas) {
          org.apache.drill.exec.store.AbstractSchema drillSchema;
          try {
            drillSchema = schema.unwrap(org.apache.drill.exec.store.AbstractSchema.class);
          } catch (ClassCastException e) {
            throw new RuntimeException(String.format("Schema '%s' is not expected under root schema", schema.getName()));
          }
          SubSchemaWrapper wrapper = new SubSchemaWrapper(drillSchema);
          thisPlus.add(wrapper.getName(), wrapper);
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

