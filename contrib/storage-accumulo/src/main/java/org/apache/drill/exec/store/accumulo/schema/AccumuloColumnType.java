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

import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Supported column types for Accumulo tables in Drill.
 *
 * <p>Accumulo is schema-less and stores all data as byte arrays.
 * This enum defines the logical types that Drill will use to interpret
 * the byte data when reading from Accumulo.</p>
 */
public enum AccumuloColumnType {

  /**
   * Variable-length string (default type).
   */
  VARCHAR(SqlTypeName.VARCHAR),

  /**
   * Fixed-length string.
   */
  CHAR(SqlTypeName.CHAR),

  /**
   * 32-bit signed integer.
   */
  INT(SqlTypeName.INTEGER),

  /**
   * Alias for INT.
   */
  INTEGER(SqlTypeName.INTEGER),

  /**
   * 64-bit signed integer.
   */
  BIGINT(SqlTypeName.BIGINT),

  /**
   * Alias for BIGINT.
   */
  LONG(SqlTypeName.BIGINT),

  /**
   * 16-bit signed integer.
   */
  SMALLINT(SqlTypeName.SMALLINT),

  /**
   * 8-bit signed integer.
   */
  TINYINT(SqlTypeName.TINYINT),

  /**
   * Single-precision floating point.
   */
  FLOAT(SqlTypeName.FLOAT),

  /**
   * Double-precision floating point.
   */
  DOUBLE(SqlTypeName.DOUBLE),

  /**
   * Exact numeric with configurable precision and scale.
   */
  DECIMAL(SqlTypeName.DECIMAL),

  /**
   * Boolean value.
   */
  BOOLEAN(SqlTypeName.BOOLEAN),

  /**
   * Date without time component.
   */
  DATE(SqlTypeName.DATE),

  /**
   * Time without date component.
   */
  TIME(SqlTypeName.TIME),

  /**
   * Date and time.
   */
  TIMESTAMP(SqlTypeName.TIMESTAMP),

  /**
   * Binary data (raw bytes).
   */
  VARBINARY(SqlTypeName.VARBINARY),

  /**
   * Dynamic type (determined at runtime).
   */
  ANY(SqlTypeName.ANY);

  private final SqlTypeName sqlTypeName;

  AccumuloColumnType(SqlTypeName sqlTypeName) {
    this.sqlTypeName = sqlTypeName;
  }

  /**
   * Returns the corresponding Calcite SQL type name.
   */
  public SqlTypeName getSqlTypeName() {
    return sqlTypeName;
  }

  /**
   * Parses a type string to an AccumuloColumnType.
   *
   * <p>Case-insensitive matching. Returns VARCHAR if the type is not recognized.</p>
   *
   * @param typeString the type string to parse
   * @return the corresponding AccumuloColumnType
   */
  public static AccumuloColumnType fromString(String typeString) {
    if (typeString == null || typeString.trim().isEmpty()) {
      return VARCHAR;
    }

    String normalized = typeString.trim().toUpperCase();

    // Handle common aliases
    switch (normalized) {
      case "STRING":
      case "TEXT":
        return VARCHAR;
      case "INT":
      case "INTEGER":
        return INTEGER;
      case "LONG":
      case "BIGINT":
        return BIGINT;
      case "FLOAT":
      case "REAL":
        return FLOAT;
      case "DOUBLE":
      case "DOUBLE PRECISION":
        return DOUBLE;
      case "BOOL":
      case "BOOLEAN":
        return BOOLEAN;
      case "BYTES":
      case "BINARY":
      case "VARBINARY":
        return VARBINARY;
      default:
        try {
          return valueOf(normalized);
        } catch (IllegalArgumentException e) {
          return VARCHAR;
        }
    }
  }

  /**
   * Returns true if this type is a numeric type.
   */
  public boolean isNumeric() {
    switch (this) {
      case INT:
      case INTEGER:
      case BIGINT:
      case LONG:
      case SMALLINT:
      case TINYINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

  /**
   * Returns true if this type is a temporal type.
   */
  public boolean isTemporal() {
    switch (this) {
      case DATE:
      case TIME:
      case TIMESTAMP:
        return true;
      default:
        return false;
    }
  }
}
