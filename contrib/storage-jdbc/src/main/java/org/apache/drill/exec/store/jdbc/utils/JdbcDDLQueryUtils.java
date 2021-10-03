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

import org.apache.calcite.config.Lex;;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

public class JdbcDDLQueryUtils {

  private static final Config DEFAULT_CONFIGURATION = SqlParser.configBuilder()
    .setCaseSensitive(true)
    .build();

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

      // HACK  See CALCITE-4820 (https://issues.apache.org/jira/browse/CALCITE-4820)
      // Calcite doesn't seem to provide a way to generate INSERT queries without the ROW
      // Keyword in front of the VALUES clause
      cleanSQL = cleanSQL.replaceAll("ROW\\(", "\\(");
      return cleanSQL;
    } catch (SqlParseException e) {
      return null;
    }
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

}
