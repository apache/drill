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

package org.apache.drill.exec.store.jdbc;

import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.drill.exec.store.jdbc.utils.JdbcDDLQueryUtils;
import org.apache.drill.exec.store.jdbc.utils.JdbcQueryBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestJdbcQueryBuilder {

  @Test
  public void testSimpleTable() {
    String table = "table";
    String schema = "schema";
    String catalog = "catalog";

    String completeTable = JdbcQueryBuilder.buildCompleteTableName(table, catalog, schema);
    assertEquals("`catalog`.`schema`.`table`", completeTable);
    assertEquals("`catalog`.`table`", JdbcQueryBuilder.buildCompleteTableName(table, catalog, ""));
    assertEquals("`catalog`.`table`", JdbcQueryBuilder.buildCompleteTableName(table, catalog, null));
  }

  @Test
  public void testTablesWithSpaces() {
    String table = "table with spaces";
    String schema = "schema with spaces";
    String catalog = "catalog with spaces";

    String completeTable = JdbcQueryBuilder.buildCompleteTableName(table, catalog, schema);
    assertEquals("`catalog with spaces`.`schema with spaces`.`table with spaces`", completeTable);
    assertEquals("`catalog with spaces`.`table with spaces`", JdbcQueryBuilder.buildCompleteTableName(table, catalog, ""));
    assertEquals("`catalog with spaces`.`table with spaces`", JdbcQueryBuilder.buildCompleteTableName(table, catalog, null));
  }

  @Test
  public void testQueryCleanup() {
    String sql = "INSERT INTO mysql_test.data_types VALUES(1, 2, 3.0, 4.0, '5.0', '2020-12-31', '12:00:00', '2015-12-30 17:55:55')";
    String cleanMySQL = JdbcDDLQueryUtils.cleanInsertQuery(sql, DatabaseProduct.MYSQL.getDialect());
    assertEquals("INSERT INTO `mysql_test`.`data_types`\n" + "VALUES  (1, 2, 3.0, 4.0, '5.0', '2020-12-31', '12:00:00', '2015-12-30 17:55:55')", cleanMySQL);

    sql = "INSERT INTO mysql_test.data_types VALUES ROW(1, 2, 3.0, 4.0, '5.0', '2020-12-31', '12:00:00', '2015-12-30 17:55:55')";
    cleanMySQL = JdbcDDLQueryUtils.cleanInsertQuery(sql, DatabaseProduct.MYSQL.getDialect());
    assertEquals("INSERT INTO `mysql_test`.`data_types`\n" + "VALUES  (1, 2, 3.0, 4.0, '5.0', '2020-12-31', '12:00:00', '2015-12-30 17:55:55')", cleanMySQL);

    String cleanMSSQL = JdbcDDLQueryUtils.cleanInsertQuery(sql, DatabaseProduct.MSSQL.getDialect());
    assertEquals("INSERT INTO [MYSQL_TEST].[DATA_TYPES]\n" + "VALUES  (1, 2, 3.0, 4.0, '5.0', '2020-12-31', '12:00:00', '2015-12-30 17:55:55')", cleanMSSQL);

    String cleanOracle = JdbcDDLQueryUtils.cleanInsertQuery(sql, DatabaseProduct.ORACLE.getDialect());
    assertEquals("INSERT INTO \"MYSQL_TEST\".\"DATA_TYPES\"\n" + "VALUES  (1, 2, 3.0, 4.0, '5.0', '2020-12-31', '12:00:00', '2015-12-30 17:55:55')", cleanOracle);

    String cleanPostgres = JdbcDDLQueryUtils.cleanInsertQuery(sql, DatabaseProduct.POSTGRESQL.getDialect());
    assertEquals("INSERT INTO \"mysql_test\".\"data_types\"\n" + "VALUES  (1, 2, 3.0, 4.0, '5.0', '2020-12-31', '12:00:00', '2015-12-30 17:55:55')", cleanPostgres);
  }
}
