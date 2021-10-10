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

import org.apache.calcite.sql.SqlDialect;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.jdbc.JdbcRecordWriter;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;

public class JdbcQueryBuilder {
  private static final Logger logger = LoggerFactory.getLogger(JdbcQueryBuilder.class);
  public static final int DEFAULT_VARCHAR_PRECISION = 100;

  private static final String CREATE_TABLE_QUERY = "CREATE TABLE %s (";
  private final StringBuilder createTableQuery;
  private SqlDialect dialect;
  private StringBuilder columns;

  public JdbcQueryBuilder(String tableName, SqlDialect dialect) {
    if (Strings.isNullOrEmpty(tableName)) {
      throw new UnsupportedOperationException("Table name cannot be empty");
    }
    this.dialect = dialect;
    createTableQuery = new StringBuilder();
    createTableQuery.append(String.format(CREATE_TABLE_QUERY, tableName));
    columns = new StringBuilder();
  }

  /**
   * Adds a column to the CREATE TABLE statement
   * @param colName The column to be added to the table
   * @param type The Drill MinorType of the column
   * @param nullable If the column is nullable or not.
   * @param precision The precision, or overall length of a column
   * @param scale The scale, or number of digits after the decimal
   */
  public void addColumn(String colName, MinorType type, boolean nullable, int precision, int scale) {
    StringBuilder queryText = new StringBuilder();
    String jdbcColType = "";
    try {
      jdbcColType = JDBCType.valueOf(JdbcRecordWriter.JDBC_TYPE_MAPPINGS.get(type)).getName();
    } catch (NullPointerException e) {
      // JDBC Does not support writing complex fields to databases
      throw UserException.dataWriteError()
        .message("Drill does not support writing complex fields to JDBC data sources.")
        .addContext(colName + " is a complex type.")
        .build(logger);
    }

    queryText.append(colName).append(" ").append(jdbcColType);

    // Add precision or scale if applicable
    if (jdbcColType.equals("VARCHAR")) {
      int max_precision = Math.max(precision, DEFAULT_VARCHAR_PRECISION);
      queryText.append("(").append(max_precision).append(")");
    }

    if (!nullable) {
      queryText.append(" NOT NULL");
    }

    if (! Strings.isNullOrEmpty(columns.toString())) {
      columns.append(",\n");
    }

    columns.append(queryText);
  }

  /**
   * Generates the CREATE TABLE query.
   * @return The create table query.
   */
  public String getCreateTableQuery() {
    createTableQuery.append(columns);
    createTableQuery.append("\n)");
    return createTableQuery.toString();
  }

  @Override
  public String toString() {
    return getCreateTableQuery();
  }

  /**
   * This function adds the appropriate catalog, schema and table for the FROM clauses for INSERT queries
   * @param table The table
   * @param catalog The database catalog
   * @param schema The database schema
   * @return The table with catalog and schema added, if present
   */
  public static String buildCompleteTableName(String table, String catalog, String schema) {
    logger.debug("Building complete table.");
    StringBuilder completeTable = new StringBuilder();
    if (! Strings.isNullOrEmpty(catalog)) {
      completeTable.append(catalog);
      completeTable.append(".");
    }

    if (! Strings.isNullOrEmpty(schema)) {
      completeTable.append(schema);
      completeTable.append(".");
    }
    completeTable.append(table);
    return JdbcDDLQueryUtils.addBackTicksToTable(completeTable.toString());
  }
}
