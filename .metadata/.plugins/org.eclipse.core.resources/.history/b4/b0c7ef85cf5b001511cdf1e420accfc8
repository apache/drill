/**
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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.calcite.linq4j.tree.DefaultExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.dotdrill.View;
import org.apache.drill.exec.planner.logical.CreateTableEntry;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public abstract class AbstractSchema implements Schema, SchemaPartitionExplorer, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSchema.class);

  protected final List<String> schemaPath;
  protected final String name;
  private static final Expression EXPRESSION = new DefaultExpression(Object.class);

  public AbstractSchema(List<String> parentSchemaPath, String name) {
    schemaPath = Lists.newArrayList();
    schemaPath.addAll(parentSchemaPath);
    schemaPath.add(name);
    this.name = name;
  }

  @Override
  public Iterable<String> getSubPartitions(String table,
                                           List<String> partitionColumns,
                                           List<String> partitionValues
                                          ) throws PartitionNotFoundException {
    throw new UnsupportedOperationException(
        String.format("Schema of type: %s " +
                      "does not support retrieving sub-partition information.",
                      this.getClass().getSimpleName()));
  }

  public String getName() {
    return name;
  }

  public List<String> getSchemaPath() {
    return schemaPath;
  }

  public String getFullSchemaName() {
    return Joiner.on(".").join(schemaPath);
  }

  public abstract String getTypeName();

  /**
   * The schema can be a top level schema which doesn't have its own tables, but refers
   * to one of the default sub schemas for table look up.
   *
   * Default implementation returns itself.
   *
   * Ex. "dfs" schema refers to the tables in "default" workspace when querying for
   * tables in "dfs" schema.
   *
   * @return Return the default schema where tables are created or retrieved from.
   */
  public AbstractSchema getDefaultSchema() {
    return this;
  }

  /**
   * Create a new view given definition.
   * @param view View info including name, definition etc.
   * @return Returns true if an existing view is replaced with the given view. False otherwise.
   * @throws IOException
   */
  public boolean createView(View view) throws IOException {
    throw UserException.unsupportedError()
        .message("Creating new view is not supported in schema [%s]", getSchemaPath())
        .build(logger);
  }

  /**
   * Drop the view with given name.
   *
   * @param viewName
   * @throws IOException
   */
  public void dropView(String viewName) throws IOException {
    throw UserException.unsupportedError()
        .message("Dropping a view is supported in schema [%s]", getSchemaPath())
        .build(logger);
  }

  /**
   *
   * @param tableName : new table name.
   * @param partitionColumns : list of partition columns. Empty list if there is no partition columns.
   * @return
   */
  public CreateTableEntry createNewTable(String tableName, List<String> partitionColumns) {
    throw UserException.unsupportedError()
        .message("Creating new tables is not supported in schema [%s]", getSchemaPath())
        .build(logger);
  }

  /**
   * Reports whether to show items from this schema in INFORMATION_SCHEMA
   * tables.
   * (Controls ... TODO:  Doc.:  Mention what this typically controls or
   * affects.)
   * <p>
   *   This base implementation returns {@code true}.
   * </p>
   */
  public boolean showInInformationSchema() {
    return true;
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return Collections.emptyList();
  }

  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public AbstractSchema getSubSchema(String name) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Table getTable(String name){
    return null;
  }

  @Override
  public Set<String> getTableNames() {
    return Collections.emptySet();
  }

  @Override
  public Expression getExpression(SchemaPlus parentSchema, String name) {
    return EXPRESSION;
  }

  @Override
  public boolean contentsHaveChangedSince(long lastCheck, long now) {
    return true;
  }

  @Override
  public void close() throws Exception {
    // no-op: default implementation for most implementations.
  }
}
