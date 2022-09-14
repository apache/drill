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

package org.apache.drill.exec.store.jdbc.utils;

import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.MaterializedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CreateTableStmtBuilder {
  private static final Logger logger = LoggerFactory.getLogger(CreateTableStmtBuilder.class);
  public static final int DEFAULT_VARCHAR_PRECISION = 100;

  private final List<String> tableIdentifier;
  private final SqlDialect dialect;
  private final SqlNodeList sqlColumns = new SqlNodeList(SqlParserPos.ZERO);

  public CreateTableStmtBuilder(List<String> tableIdentifier, SqlDialect dialect) {
    if (CollectionUtils.isEmpty(tableIdentifier)) {
      throw new UnsupportedOperationException("Table name cannot be empty");
    }
    this.tableIdentifier = tableIdentifier;
    this.dialect = dialect;
  }

  /**
   * Adds a column to the CREATE TABLE statement
   */
  public void addColumn(MaterializedField field) {
    TypeProtos.MajorType majorType = populateScaleAndPrecisionIfRequired(field.getType());
    int jdbcType = Types.getJdbcTypeCode(Types.getSqlTypeName(majorType));
    if (dialect instanceof PostgresqlSqlDialect) {
      // pg data type name special case
      if (jdbcType == java.sql.Types.DOUBLE) {
        // TODO: Calcite will incorrectly output DOUBLE instead of DOUBLE PRECISION under the pg dialect
        jdbcType = java.sql.Types.FLOAT;
      }
    }
    SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(jdbcType);

    if (sqlTypeName == null) {
      throw UserException.dataWriteError()
        .message("Drill does not support writing complex fields to JDBC data sources.")
        .addContext(field.getName() + " is a complex type.")
        .build(logger);
    }

    int precision = majorType.hasPrecision()
      ? majorType.getPrecision()
      : -1;

    int scale = majorType.hasScale()
      ? majorType.getScale()
      : -1;

    SqlBasicTypeNameSpec typeNameSpec = new SqlBasicTypeNameSpec(
      sqlTypeName, precision, scale, SqlParserPos.ZERO);

    SqlDataTypeSpec sqlDataTypeSpec = new SqlDataTypeSpec(
      typeNameSpec,
      SqlParserPos.ZERO).withNullable(field.isNullable());

    ColumnStrategy columnStrategy = field.isNullable()
      ? ColumnStrategy.NULLABLE
      : ColumnStrategy.NOT_NULLABLE;

    SqlNode sqlColumnDeclaration = SqlDdlNodes.column(SqlParserPos.ZERO,
      new SqlIdentifier(field.getName(), SqlParserPos.ZERO),
      sqlDataTypeSpec,
      null,
      columnStrategy);

    sqlColumns.add(sqlColumnDeclaration);
  }

  /**
   * Generates the CREATE TABLE query.
   * @return The create table query.
   */
  public String build() {
    SqlIdentifier sqlIdentifier = new SqlIdentifier(tableIdentifier, SqlParserPos.ZERO);
    SqlNode createTable = SqlDdlNodes.createTable(SqlParserPos.ZERO,
      false, false, sqlIdentifier, sqlColumns, null);

    return createTable.toSqlString(dialect, true).getSql();
  }

  private static TypeProtos.MajorType populateScaleAndPrecisionIfRequired(TypeProtos.MajorType type) {
    switch (type.getMinorType()) {
      case VARDECIMAL:
        if (!type.hasPrecision()) {
          type = type.toBuilder().setPrecision(Types.maxPrecision(type.getMinorType())).build();
        }
        if (!type.hasScale()) {
          type = type.toBuilder().setScale(0).build();
        }
        break;
      case FIXEDCHAR:
      case FIXED16CHAR:
      case VARCHAR:
      case VAR16CHAR:
      case VARBINARY:
        if (!type.hasPrecision()) {
          type = type.toBuilder().setPrecision(DEFAULT_VARCHAR_PRECISION).build();
        }
      case TIMESTAMP:
      case TIME:
        if (!type.hasPrecision()) {
          type = type.toBuilder().setPrecision(Types.DEFAULT_TIMESTAMP_PRECISION).build();
        }
      default:
    }
    return type;
  }
}
