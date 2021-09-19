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

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Ordering;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.jdbc.JdbcRecordWriter;
import org.apache.parquet.Strings;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class JdbcQueryBuilder {

  private static final Config DEFAULT_CONFIGURATION = SqlParser.configBuilder()
    .setCaseSensitive(true)
    .build();

  private static final Ordering<Iterable<Integer>> VERSION_ORDERING =
    Ordering.<Integer>natural().lexicographical();

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
   * @param connection The database connection
   * @param table The table
   * @return The table with catalog and schema added, if present
   */
  public static String buildCompleteTableName(Connection connection, String table) {
    StringBuilder completeTable = new StringBuilder();
    try {
      Pair<String, String> catalogSchema = getCatalogSchema(connection);
      if (catalogSchema.left != null) {
        completeTable.append(catalogSchema.left);
        completeTable.append(".");
      }
      if (catalogSchema.right != null) {
        completeTable.append(".");
        completeTable.append(catalogSchema.right);
        completeTable.append(".");
      }
    } catch (SQLException e) {
      // Do nothing
    }
    completeTable.append(table);
    return completeTable.toString();
  }

  private static Pair<String, String> getCatalogSchema(Connection connection)
    throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    final List<Integer> version41 = ImmutableList.of(4, 1); // JDBC 4.1
    String catalog = null;
    String schema = null;
    final boolean jdbc41OrAbove =
      VERSION_ORDERING.compare(version(metaData), version41) >= 0;
    if (jdbc41OrAbove) {
      // From JDBC 4.1, catalog and schema can be retrieved from the connection
      // object, hence try to get it from there if it was not specified by user
      catalog = connection.getCatalog();
    }
    if (jdbc41OrAbove) {
      schema = connection.getSchema();
      if ("".equals(schema)) {
        schema = null; // PostgreSQL returns useless "" sometimes
      }
    }
    if ((catalog == null || schema == null)
      && metaData.getDatabaseProductName().equals("PostgreSQL")) {
      final String sql = "SELECT current_database(), current_schema()";
      try (Statement statement = connection.createStatement();
           ResultSet resultSet = statement.executeQuery(sql)) {
        if (resultSet.next()) {
          catalog = resultSet.getString(1);
          schema = resultSet.getString(2);
        }
      }
    }
    return Pair.of(catalog, schema);
  }

  private static List<Integer> version(DatabaseMetaData metaData) throws SQLException {
    return ImmutableList.of(metaData.getJDBCMajorVersion(),
      metaData.getJDBCMinorVersion());
  }
}
