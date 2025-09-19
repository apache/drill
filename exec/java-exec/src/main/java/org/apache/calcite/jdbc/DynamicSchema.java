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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Named;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.alias.AliasRegistryProvider;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.util.Set;
import com.google.common.collect.ImmutableList;

/**
 * Unlike SimpleCalciteSchema, DynamicSchema could have an empty or partial schemaMap, but it could maintain a map of
 * name->SchemaFactory, and only register schema when the correspondent name is requested.
 */
public class DynamicSchema extends SimpleCalciteSchema implements AutoCloseable {

  public DynamicSchema(CalciteSchema parent, Schema schema, String name) {
    super(parent, schema, name);
  }

  @Override
  protected CalciteSchema createSubSchema(Schema schema, String name) {
    return new DynamicSchema(this, schema, name);
  }

  public static SchemaPlus createRootSchema(StoragePluginRegistry storages,
      SchemaConfig schemaConfig, AliasRegistryProvider aliasRegistryProvider) {
    DynamicRootSchema rootSchema = new DynamicRootSchema(storages, schemaConfig, aliasRegistryProvider);
    return rootSchema.plus();
  }

  @Override
  public CalciteSchema add(String name, Schema schema) {
    CalciteSchema calciteSchema =
      new DynamicSchema(this, schema, name);
    subSchemaMap.put(name, calciteSchema);
    return calciteSchema;
  }

  @Override
  public Lookup<TableEntry> tables() {
    Lookup<TableEntry> baseLookup = super.tables();
    return new ImplicitTableLookup(baseLookup);
  }

  private class ImplicitTableLookup implements Lookup<TableEntry> {
    private final Lookup<TableEntry> baseLookup;

    ImplicitTableLookup(Lookup<TableEntry> baseLookup) {
      this.baseLookup = baseLookup;
    }

    @Override
    public TableEntry get(String tableName) {
      // First check the base lookup for existing tables
      TableEntry existing = baseLookup.get(tableName);
      if (existing != null) {
        return existing;
      }

      // Then try to get implicit table
      return getImplicitTableBasedOnNullaryFunction(tableName, false);
    }

    @Override
    public Named<TableEntry> getIgnoreCase(String tableName) {
      Named<TableEntry> existing = baseLookup.getIgnoreCase(tableName);
      if (existing != null) {
        return existing;
      }

      TableEntry table = getImplicitTableBasedOnNullaryFunction(tableName, true);
      return table != null ? new Named<>(tableName, table) : null;
    }

    @Override
    public Set<String> getNames(LikePattern pattern) {
      // For now, just return the base lookup names
      // Implicit tables are typically discovered on-demand
      return baseLookup.getNames(pattern);
    }
  }

  @Override
  protected TableEntry getImplicitTableBasedOnNullaryFunction(String tableName, boolean caseSensitive) {
    // Delegate to the underlying schema's getTable method
    Table table = schema.getTable(tableName);
    if (table != null) {
      // Create a TableEntry for the table
      return new CalciteSchema.TableEntryImpl(this, tableName, table, ImmutableList.of());
    }
    return null;
  }

  @Override
  public void close() throws Exception {
    for (CalciteSchema cs : subSchemaMap.map().values()) {
      AutoCloseables.closeWithUserException(cs.plus().unwrap(AbstractSchema.class));
    }
  }

}
