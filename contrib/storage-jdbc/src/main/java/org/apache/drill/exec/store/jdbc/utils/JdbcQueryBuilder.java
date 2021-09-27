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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.jdbc.JdbcRecordWriter;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;

public class JdbcQueryBuilder {
  private static final Logger logger = LoggerFactory.getLogger(JdbcQueryBuilder.class);
  private static final Config DEFAULT_CONFIGURATION = SqlParser.configBuilder()
    .setCaseSensitive(true)
    .build();

  private static final String CREATE_TABLE_QUERY = "CREATE TABLE %s (";
  private final StringBuilder createTableQuery;
  private SqlDialect dialect;
  private String columns;

  public JdbcQueryBuilder(String tableName, SqlDialect dialect) {
    if (Strings.isNullOrEmpty(tableName)) {
      throw new UnsupportedOperationException("Table name cannot be empty");
    }
    this.dialect = dialect;
    createTableQuery = new StringBuilder();
    createTableQuery.append(String.format(CREATE_TABLE_QUERY, tableName));
    columns = "";
  }

  // TODO Add Precision/Scale?

  /**
   * Adds a column to the CREATE TABLE statement
   * @param colName The column to be added to the table
   * @param type The Drill MinorType of the column
   * @param nullable If the column is nullable or not.
   */
  public void addColumn(String colName, MinorType type, boolean nullable) {
    String jdbcColType = JDBCType.valueOf(JdbcRecordWriter.JDBC_TYPE_MAPPINGS.get(type)).getName();
    String queryText = colName + " " + jdbcColType;

    if (!nullable) {
      queryText += " NOT NULL";
    }

    if (!Strings.isNullOrEmpty(columns)) {
      columns += ",\n";
    }

    columns += queryText;
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
   * Converts a given SQL query from the generic dialect to the destination system dialect.  Returns
   * null if the original query is not valid.
   *
   * @param sql An ANSI SQL statement
   * @param dialect The destination system dialect
   * @return A representation of the original query in the destination dialect
   */
  public static String convertToDestinationDialect(String sql, SqlDialect dialect) {
    // TODO Fix this... it is adding additional rubbish which is invalidating the query
    try {
      SqlNode node = SqlParser.create(sql, DEFAULT_CONFIGURATION).parseQuery();
      return node.toSqlString(dialect).getSql();
    } catch (SqlParseException e) {
      // Do nothing...
    }
    return null;
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
    return completeTable.toString();
  }
}
