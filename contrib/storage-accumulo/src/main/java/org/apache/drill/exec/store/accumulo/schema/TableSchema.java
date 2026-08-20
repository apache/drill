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
package org.apache.drill.exec.store.accumulo.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the schema of an Accumulo table for Drill.
 *
 * <p>Contains the table name, row key type, and column definitions.
 * The schema is used by Drill's query planner to understand the structure
 * of Accumulo tables.</p>
 */
public class TableSchema {

  public static final String ROW_KEY_COLUMN = "row_key";

  private final String tableName;
  private final AccumuloColumnType rowKeyType;
  private final List<ColumnDef> columns;
  private final Map<String, ColumnDef> columnsByName;
  private final Map<String, ColumnDef> columnsByAccumuloKey;

  @JsonCreator
  public TableSchema(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("rowKeyType") AccumuloColumnType rowKeyType,
      @JsonProperty("columns") List<ColumnDef> columns) {
    this.tableName = tableName;
    this.rowKeyType = rowKeyType != null ? rowKeyType : AccumuloColumnType.VARBINARY;
    this.columns = columns != null ? new ArrayList<>(columns) : new ArrayList<>();

    // Build lookup maps
    this.columnsByName = new LinkedHashMap<>();
    this.columnsByAccumuloKey = new LinkedHashMap<>();
    for (ColumnDef col : this.columns) {
      columnsByName.put(col.getName().toLowerCase(), col);
      columnsByAccumuloKey.put(col.getFullColumnName(), col);
    }
  }

  /**
   * Creates a builder for constructing a TableSchema.
   */
  public static Builder builder(String tableName) {
    return new Builder(tableName);
  }

  /**
   * Creates a dynamic schema with just the row key and a wildcard columns map.
   * Used when no explicit schema is defined.
   */
  public static TableSchema dynamic(String tableName) {
    return new TableSchema(tableName, AccumuloColumnType.VARBINARY, Collections.emptyList());
  }

  @JsonProperty("tableName")
  public String getTableName() {
    return tableName;
  }

  @JsonProperty("rowKeyType")
  public AccumuloColumnType getRowKeyType() {
    return rowKeyType;
  }

  @JsonProperty("columns")
  public List<ColumnDef> getColumns() {
    return Collections.unmodifiableList(columns);
  }

  /**
   * Returns the column definition by Drill column name.
   */
  @JsonIgnore
  public ColumnDef getColumnByName(String name) {
    return columnsByName.get(name.toLowerCase());
  }

  /**
   * Returns the column definition by Accumulo column key (family:qualifier).
   */
  @JsonIgnore
  public ColumnDef getColumnByAccumuloKey(String accumuloKey) {
    return columnsByAccumuloKey.get(accumuloKey);
  }

  /**
   * Returns true if this schema has explicit column definitions.
   * If false, the schema is dynamic and will discover columns at runtime.
   */
  @JsonIgnore
  public boolean hasExplicitColumns() {
    return !columns.isEmpty();
  }

  /**
   * Returns the number of columns (excluding row_key).
   */
  @JsonIgnore
  public int getColumnCount() {
    return columns.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableSchema that = (TableSchema) o;
    return Objects.equals(tableName, that.tableName)
        && rowKeyType == that.rowKeyType
        && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, rowKeyType, columns);
  }

  @Override
  public String toString() {
    return "TableSchema{" +
        "tableName='" + tableName + '\'' +
        ", rowKeyType=" + rowKeyType +
        ", columnCount=" + columns.size() +
        '}';
  }

  /**
   * Builder for constructing TableSchema instances.
   */
  public static class Builder {
    private final String tableName;
    private AccumuloColumnType rowKeyType = AccumuloColumnType.VARBINARY;
    private final List<ColumnDef> columns = new ArrayList<>();

    private Builder(String tableName) {
      this.tableName = tableName;
    }

    public Builder rowKeyType(AccumuloColumnType type) {
      this.rowKeyType = type;
      return this;
    }

    public Builder addColumn(ColumnDef column) {
      this.columns.add(column);
      return this;
    }

    public Builder addColumn(String name, String family, String qualifier, AccumuloColumnType type) {
      this.columns.add(ColumnDef.create(name, family, qualifier, type));
      return this;
    }

    public Builder addVarcharColumn(String name, String family, String qualifier) {
      this.columns.add(ColumnDef.varchar(name, family, qualifier));
      return this;
    }

    public TableSchema build() {
      return new TableSchema(tableName, rowKeyType, columns);
    }
  }
}
