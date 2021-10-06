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

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDDLQueryUtils {

  private static final Logger logger = LoggerFactory.getLogger(JdbcDDLQueryUtils.class);
  /**
   * Converts a given SQL query from the generic dialect to the destination system dialect.  Returns
   * null if the original query is not valid.
   *
   * @param query An ANSI SQL statement
   * @param dialect The destination system dialect
   * @return A representation of the original query in the destination dialect
   */
  public static String cleanDDLQuery(String query, SqlDialect dialect) {
    SqlParser.Config sqlParserConfig = SqlParser.configBuilder()
      .setParserFactory(SqlDdlParserImpl.FACTORY)
      .setConformance(SqlConformanceEnum.MYSQL_5)
      .setCaseSensitive(true)
      .setLex(Lex.MYSQL_ANSI)
      .build();

    try {
      SqlNode node = SqlParser.create(query, sqlParserConfig).parseQuery();
      String cleanSQL =  node.toSqlString(dialect).getSql();

      // TODO Fix this hack
      // HACK  See CALCITE-4820 (https://issues.apache.org/jira/browse/CALCITE-4820)
      // Calcite doesn't seem to provide a way to generate INSERT queries without the ROW
      // Keyword in front of the VALUES clause.
      cleanSQL = cleanSQL.replaceAll("ROW\\(", "\\(");
      return cleanSQL;
    } catch (SqlParseException e) {
      logger.error(e.getMessage());
      return null;
    }
  }

  private static void appendEscapedSQLString(StringBuilder sb, String sqlString) {
    sb.append('\'');
    if (sqlString.indexOf('\'') != -1) {
      int length = sqlString.length();
      for (int i = 0; i < length; i++) {
        char c = sqlString.charAt(i);
        if (c == '\'') {
          sb.append('\'');
        }
        sb.append(c);
      }
    } else
      sb.append(sqlString);
    sb.append('\'');
  }

  /**
   * This function cleans up strings for use in INSERT queries
   * @param value The input string
   * @return The input string cleaned for SQL INSERT queries
   */
  public static String sqlEscapeString(String value) {
    StringBuilder escaper = new StringBuilder();
    appendEscapedSQLString(escaper, value);
    return escaper.toString();
  }
}
