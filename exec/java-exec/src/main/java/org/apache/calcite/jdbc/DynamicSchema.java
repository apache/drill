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
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.schema.lookup.Named;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.alias.AliasRegistryProvider;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.util.Set;


/**
 * Unlike SimpleCalciteSchema, DynamicSchema could have an empty or partial schemaMap, but it could maintain a map of
 * name->SchemaFactory, and only register schema when the correspondent name is requested.
 */
public class DynamicSchema extends SimpleCalciteSchema implements AutoCloseable {

  public DynamicSchema(CalciteSchema parent, Schema schema, String name) {
    super(parent, schema, name);
  }

  /**
   * Ensures sub-schemas materialised from the underlying {@link Schema} remain
   * {@link DynamicSchema} instances so that lazy loading keeps working at every
   * level of the schema tree. Prior to Calcite 1.39 (CALCITE-6029) this was done
   * by overriding the now-removed {@code getImplicitSubSchema(String, boolean)};
   * the new {@code subSchemas()} lookup uses {@code createSubSchema} to wrap
   * schemas resolved from the underlying {@link Schema}.
   */
  @Override
  protected CalciteSchema createSubSchema(Schema schema, String name) {
    return new DynamicSchema(this, schema, name);
  }

  /**
   * Forces case-sensitive (exact) table lookups. Drill's storage schemas resolve
   * table names themselves (for example file names within a workspace); allowing
   * Calcite to perform a case-insensitive match would force it to enumerate every
   * table in the schema (a potentially expensive directory listing). This used to
   * be expressed as {@code getImplicitTable(tableName, true)} before that method
   * was removed in Calcite 1.39.
   */
  @Override
  public Lookup<TableEntry> tables() {
    return exactLookup(super.tables());
  }

  /**
   * Wraps a {@link Lookup} so that case-insensitive lookups behave like exact,
   * case-sensitive ones: {@code getIgnoreCase} only returns a match when an
   * entry with exactly the requested name exists, avoiding any enumeration of
   * the underlying entries.
   */
  static <T> Lookup<T> exactLookup(Lookup<T> delegate) {
    return new Lookup<T>() {
      @Override
      public T get(String name) {
        return delegate.get(name);
      }

      @Override
      public Named<T> getIgnoreCase(String name) {
        T entity = delegate.get(name);
        return entity == null ? null : new Named<>(name, entity);
      }

      @Override
      public Set<String> getNames(LikePattern pattern) {
        return delegate.getNames(pattern);
      }
    };
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
  public void close() throws Exception {
    for (CalciteSchema cs : subSchemaMap.map().values()) {
      AutoCloseables.closeWithUserException(cs.plus().unwrap(AbstractSchema.class));
    }
  }

}
