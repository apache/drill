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
import org.junit.Test;

public class TestQueryUtils {

  @Test
  public void testCreateTable() {
    String sql = "CREATE TABLE drill_mysql_test.data_types (\n" +
      "int_field INTEGER NOT NULL,\n" +
      "bigint_field BIGINT NOT NULL,\n" +
      "float4_field FLOAT NOT NULL,\n" +
      "float8_field DOUBLE NOT NULL,\n" +
      "varchar_field VARCHAR(100) NOT NULL,\n" +
      "date_field DATE NOT NULL,\n" +
      "time_field TIME NOT NULL,\n" +
      "timestamp_field TIMESTAMP NOT NULL)";

    String clean = JdbcDDLQueryUtils.cleanDDLQuery(sql, DatabaseProduct.POSTGRESQL.getDialect());
    System.out.println(clean);

    clean = JdbcDDLQueryUtils.cleanDDLQuery(sql, DatabaseProduct.MYSQL.getDialect());
    System.out.println(clean);

    clean = JdbcDDLQueryUtils.cleanDDLQuery(sql, DatabaseProduct.MSSQL.getDialect());
    System.out.println(clean);
  }

  @Test
  public void testInsertRows() {
    String sql = "INSERT INTO drill_mysql_test.data_types VALUES\n" +
      "(1, 2, 3.0, 4.0, '5.0', '2020-12-31', '12:00:00', '2015-12-30 17:55:55')";

    String clean = JdbcDDLQueryUtils.cleanDDLQuery(sql, DatabaseProduct.POSTGRESQL.getDialect());
    System.out.println("Postgres: " + clean);

    clean = JdbcDDLQueryUtils.cleanDDLQuery(sql, DatabaseProduct.MYSQL.getDialect());
    System.out.println("MySQL: " + clean);

    clean = JdbcDDLQueryUtils.cleanDDLQuery(sql, DatabaseProduct.MSSQL.getDialect());
    System.out.println("MSSQL: " + clean);
  }
}
