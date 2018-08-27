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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.calcite.linq4j.tree.DefaultExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.dotdrill.View;
import org.apache.drill.exec.planner.logical.CreateTableEntry;

import org.apache.drill.shaded.guava.com.google.common.base.Joiner;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public abstract class AbstractSchema implements Schema, SchemaPartitionExplorer, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSchema.class);

  protected final List<String> schemaPath;
  protected final String name;
  private static final Expression EXPRESSION = new DefaultExpression(Object.class);

  public AbstractSchema(List<String> parentSchemaPath, String name) {
    name = name == null ? null : name.toLowerCase();
    schemaPath = new ArrayList<>();
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
  public Schema getDefaultSchema() {
    return this;
  }

  /**
   * Create a new view given definition.
   * @param view View info including name, definition etc.
   * @return Returns true if an existing view is replaced with the given view. False otherwise.
   * @throws IOException in case of error creating a view
   */
  public boolean createView(View view) throws IOException {
    throw UserException.unsupportedError()
        .message("Creating new view is not supported in schema [%s]", getSchemaPath())
        .build(logger);
  }

  /**
   * Drop the view with given name.
   *
   * @param viewName view name
   * @throws IOException in case of error dropping the view
   */
  public void dropView(String viewName) throws IOException {
    throw UserException.unsupportedError()
        .message("Dropping a view is supported in schema [%s]", getSchemaPath())
        .build(logger);
  }

  /**
   * Creates table entry using table name, list of partition columns
   * and storage strategy used to create table folder and files
   *
   * @param tableName : new table name.
   * @param partitionColumns : list of partition columns. Empty list if there is no partition columns.
   * @param storageStrategy : storage strategy used to create table folder and files
   * @return create table entry
   */
  public CreateTableEntry createNewTable(String tableName, List<String> partitionColumns, StorageStrategy storageStrategy) {
    throw UserException.unsupportedError()
        .message("Creating new tables is not supported in schema [%s]", getSchemaPath())
        .build(logger);
  }

  /**
   * Creates table entry using table name and list of partition columns if any.
   * Table folder and files will be created using persistent storage strategy.
   *
   * @param tableName : new table name.
   * @param partitionColumns : list of partition columns. Empty list if there is no partition columns.
   * @return create table entry
   */
  public CreateTableEntry createNewTable(String tableName, List<String> partitionColumns) {
    return createNewTable(tableName, partitionColumns, StorageStrategy.DEFAULT);
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

  /**
   * Returns a map of types in this schema by name.
   *
   * <p>The implementations of {@link #getTypeNames()}
   * and {@link #getType(String)} depend on this map.
   * The default implementation of this method returns the empty map.
   * Override this method to change their behavior.</p>
   *
   * @return Map of types in this schema by name
   */
  protected Map<String, RelProtoDataType> getTypeMap() {
    return ImmutableMap.of();
  }

  @Override
  public Set<String> getTypeNames() {
    return getTypeMap().keySet();
  }

  @Override
  public RelProtoDataType getType(String name) {
    return getTypeMap().get(name);
  }

  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public Schema getSubSchema(String name) {
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
  public void close() throws Exception {
    // no-op: default implementation for most implementations.
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }

  public void dropTable(String tableName) {
    throw UserException.unsupportedError()
        .message("Dropping tables is not supported in schema [%s]", getSchemaPath())
        .build(logger);
  }

  /**
   * Get the collection of {@link Table} tables specified in the tableNames with bulk-load (if the underlying storage
   * plugin supports).
   * It is not guaranteed that the retrieved tables would have RowType and Statistic being fully populated.
   *
   * Specifically, calling {@link Table#getRowType(org.apache.calcite.rel.type.RelDataTypeFactory)} or {@link Table#getStatistic()} might incur
   * {@link UnsupportedOperationException} being thrown.
   *
   * @param  tableNames the requested tables, specified by the table names
   * @return the collection of requested tables
   */
  public List<Pair<String, ? extends Table>> getTablesByNamesByBulkLoad(final List<String> tableNames, int bulkSize) {
    return getTablesByNames(tableNames);
  }

  /**
   * Get the collection of {@link Table} tables specified in the tableNames.
   *
   * @param  tableNames the requested tables, specified by the table names
   * @return the collection of requested tables
   */
  public List<Pair<String, ? extends Table>> getTablesByNames(final List<String> tableNames) {
    final List<Pair<String, ? extends Table>> tables = Lists.newArrayList();
    for (String tableName : tableNames) {
      final Table table = getTable(tableName);
      if (table == null) {
        // Schema may return NULL for table if the query user doesn't have permissions to load the table. Ignore such
        // tables as INFO SCHEMA is about showing tables which the use has access to query.
        continue;
      }
      tables.add(Pair.of(tableName, table));
    }
    return tables;
  }

  public List<Pair<String, Schema.TableType>> getTableNamesAndTypes(boolean bulkLoad, int bulkSize) {
    final List<String> tableNames = Lists.newArrayList(getTableNames());
    final List<Pair<String, Schema.TableType>> tableNamesAndTypes = Lists.newArrayList();
    final List<Pair<String, ? extends Table>> tables;
    if (bulkLoad) {
      tables = getTablesByNamesByBulkLoad(tableNames, bulkSize);
    } else {
      tables = getTablesByNames(tableNames);
    }
    for (Pair<String, ? extends Table> table : tables) {
      tableNamesAndTypes.add(Pair.of(table.getKey(), table.getValue().getJdbcTableType()));
    }

    return tableNamesAndTypes;
  }

  /**
   * Indicates if table names in schema are case sensitive. By default they are.
   * If schema implementation claims its table names are case insensitive,
   * it is responsible for making case insensitive look up by table name.
   *
   * @return true if table names are case sensitive
   */
  public boolean areTableNamesCaseSensitive() {
    return true;
  }

}
