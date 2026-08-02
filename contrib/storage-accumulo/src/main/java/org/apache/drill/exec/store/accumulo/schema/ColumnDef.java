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

import java.util.Objects;

import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a column definition for an Accumulo table in Drill.
 *
 * <p>Maps an Accumulo column family:qualifier pair to a Drill column with
 * a specific SQL type.</p>
 */
public class ColumnDef {

  private final String name;
  private final String columnFamily;
  private final String columnQualifier;
  private final AccumuloColumnType type;
  private final boolean nullable;

  @JsonCreator
  public ColumnDef(
      @JsonProperty("name") String name,
      @JsonProperty("columnFamily") String columnFamily,
      @JsonProperty("columnQualifier") String columnQualifier,
      @JsonProperty("type") AccumuloColumnType type,
      @JsonProperty("nullable") Boolean nullable) {
    this.name = name;
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
    this.type = type != null ? type : AccumuloColumnType.VARCHAR;
    this.nullable = nullable != null ? nullable : true;
  }

  /**
   * Convenience constructor for creating a column definition.
   */
  public static ColumnDef create(String name, String columnFamily, String columnQualifier,
      AccumuloColumnType type) {
    return new ColumnDef(name, columnFamily, columnQualifier, type, true);
  }

  /**
   * Convenience constructor for VARCHAR columns.
   */
  public static ColumnDef varchar(String name, String columnFamily, String columnQualifier) {
    return new ColumnDef(name, columnFamily, columnQualifier, AccumuloColumnType.VARCHAR, true);
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("columnFamily")
  public String getColumnFamily() {
    return columnFamily;
  }

  @JsonProperty("columnQualifier")
  public String getColumnQualifier() {
    return columnQualifier;
  }

  @JsonProperty("type")
  public AccumuloColumnType getType() {
    return type;
  }

  @JsonProperty("nullable")
  public boolean isNullable() {
    return nullable;
  }

  /**
   * Returns the SQL type name for this column.
   */
  @JsonIgnore
  public SqlTypeName getSqlTypeName() {
    return type.getSqlTypeName();
  }

  /**
   * Returns the full Accumulo column identifier (family:qualifier).
   */
  @JsonIgnore
  public String getFullColumnName() {
    if (columnQualifier == null || columnQualifier.isEmpty()) {
      return columnFamily;
    }
    return columnFamily + ":" + columnQualifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnDef columnDef = (ColumnDef) o;
    return nullable == columnDef.nullable
        && Objects.equals(name, columnDef.name)
        && Objects.equals(columnFamily, columnDef.columnFamily)
        && Objects.equals(columnQualifier, columnDef.columnQualifier)
        && type == columnDef.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, columnFamily, columnQualifier, type, nullable);
  }

  @Override
  public String toString() {
    return "ColumnDef{" +
        "name='" + name + '\'' +
        ", columnFamily='" + columnFamily + '\'' +
        ", columnQualifier='" + columnQualifier + '\'' +
        ", type=" + type +
        ", nullable=" + nullable +
        '}';
  }
}
