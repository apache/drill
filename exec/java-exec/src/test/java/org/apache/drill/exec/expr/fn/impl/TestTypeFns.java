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
package org.apache.drill.exec.expr.fn.impl;

import static org.junit.Assert.assertEquals;

import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTypeFns extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    // Use the following three lines if you add a function
    // to avoid the need for a full Drill build.
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .configProperty("drill.classpath.scanning.cache.enabled", false);
    startCluster(builder);

    // Use the following line if a full Drill build has been
    // done since adding new functions.
//    startCluster(ClusterFixture.builder(dirTestWatcher).maxParallelization(1));
  }

  @Test
  public void testTypeOf() throws RpcException {
    // SMALLINT not supported in CAST
    //doTypeOfTest("SMALLINT");
    doTypeOfTest("INT");
    doTypeOfTest("BIGINT");
    doTypeOfTest("VARCHAR");
    doTypeOfTest("FLOAT", "FLOAT4");
    doTypeOfTest("DOUBLE", "FLOAT8");
    doTypeOfTestSpecial("a", "true", "BIT");
    doTypeOfTestSpecial("a", "CURRENT_DATE", "DATE");
    doTypeOfTestSpecial("a", "CURRENT_TIME", "TIME");
    doTypeOfTestSpecial("a", "CURRENT_TIMESTAMP", "TIMESTAMP");
    doTypeOfTestSpecial("a", "AGE(CURRENT_TIMESTAMP)", "INTERVAL");
    doTypeOfTestSpecial("BINARY_STRING(a)", "'\\xde\\xad\\xbe\\xef'", "VARBINARY");
    try {
      client.alterSession("planner.enable_decimal_data_type", true);
      doTypeOfTestSpecial("CAST(a AS DECIMAL)", "1", "DECIMAL38SPARSE");
      doTypeOfTestSpecial("CAST(a AS DECIMAL(6, 3))", "1", "DECIMAL9");
    } finally {
      client.resetSession("planner.enable_decimal_data_type");
    }
  }

  private void doTypeOfTest(String type) throws RpcException {
    doTypeOfTest(type, type);
  }

  private void doTypeOfTest(String castType, String resultType) throws RpcException {

    // typeof() returns types using the internal names.

    String sql = "SELECT typeof(CAST(a AS " + castType + ")) FROM (VALUES (1)) AS T(a)";
    String result = client.queryBuilder().sql(sql).singletonString();
    assertEquals(resultType, result);

    // For typeof(), null values annoyingly report a type of "NULL"

    sql = "SELECT typeof(CAST(a AS " + castType + ")) FROM cp.`functions/null.json`";
    result = client.queryBuilder().sql(sql).singletonString();
    assertEquals("NULL", result);
  }

  private void doTypeOfTestSpecial(String expr, String value, String resultType) throws RpcException {
    String sql = "SELECT typeof(" + expr + ") FROM (VALUES (" + value + ")) AS T(a)";
    String result = client.queryBuilder().sql(sql).singletonString();
    assertEquals(resultType, result);
  }

  @Test
  public void testSqlTypeOf() throws RpcException {
    // SMALLINT not supported in CAST
    //doSqlTypeOfTest("SMALLINT");
    doSqlTypeOfTest("INTEGER");
    doSqlTypeOfTest("BIGINT");
    doSqlTypeOfTest("CHARACTER VARYING");
    doSqlTypeOfTest("FLOAT");
    doSqlTypeOfTest("DOUBLE");
    doSqlTypeOfTestSpecial("a", "true", "BOOLEAN");
    doSqlTypeOfTestSpecial("a", "CURRENT_DATE", "DATE");
    doSqlTypeOfTestSpecial("a", "CURRENT_TIME", "TIME");
    doSqlTypeOfTestSpecial("a", "CURRENT_TIMESTAMP", "TIMESTAMP");
    doSqlTypeOfTestSpecial("a", "AGE(CURRENT_TIMESTAMP)", "INTERVAL");
    doSqlTypeOfTestSpecial("BINARY_STRING(a)", "'\\xde\\xad\\xbe\\xef'", "BINARY VARYING");
    try {
      client.alterSession("planner.enable_decimal_data_type", true);

      // These should include precision and scale: DECIMAL(p, s)
      // But, see DRILL-6378

      doSqlTypeOfTestSpecial("CAST(a AS DECIMAL)", "1", "DECIMAL");
      doSqlTypeOfTestSpecial("CAST(a AS DECIMAL(6, 3))", "1", "DECIMAL");
    } finally {
      client.resetSession("planner.enable_decimal_data_type");
    }
  }

  private void doSqlTypeOfTest(String type) throws RpcException {

    // sqlTypeOf() returns SQL type names: the names used in CAST.

    String sql = "SELECT sqlTypeOf(CAST(a AS " + type + ")) FROM (VALUES (1)) AS T(a)";
    String result = client.queryBuilder().sql(sql).singletonString();
    assertEquals(type, result);

    // Returns same type even value is null.

    sql = "SELECT sqlTypeOf(CAST(a AS " + type + ")) FROM cp.`functions/null.json`";
    result = client.queryBuilder().sql(sql).singletonString();
    assertEquals(type, result);
  }

  private void doSqlTypeOfTestSpecial(String expr, String value, String resultType) throws RpcException {
    String sql = "SELECT sqlTypeof(" + expr + ") FROM (VALUES (" + value + ")) AS T(a)";
    String result = client.queryBuilder().sql(sql).singletonString();
    assertEquals(resultType, result);
  }

  @Test
  public void testDrillTypeOf() throws RpcException {
    // SMALLINT not supported in CAST
    //doDrillTypeOfTest("SMALLINT");
    doDrillTypeOfTest("INTEGER", "INT");
    doDrillTypeOfTest("BIGINT");
    doDrillTypeOfTest("CHARACTER VARYING", "VARCHAR");
    doDrillTypeOfTest("FLOAT", "FLOAT4");
    doDrillTypeOfTest("DOUBLE", "FLOAT8");

    // Omitting the other types. Internal code is identical to
    // typeof() except for null handling.
  }

  private void doDrillTypeOfTest(String type) throws RpcException {
    doDrillTypeOfTest(type, type);
  }

  private void doDrillTypeOfTest(String castType, String resultType) throws RpcException {

    // drillTypeOf() returns types using the internal names.

    String sql = "SELECT drillTypeOf(CAST(a AS " + castType + ")) FROM (VALUES (1)) AS T(a)";
    String result = client.queryBuilder().sql(sql).singletonString();
    assertEquals(resultType, result);

    // Returns same type even value is null.

    sql = "SELECT drillTypeOf(CAST(a AS " + castType + ")) FROM cp.`functions/null.json`";
    result = client.queryBuilder().sql(sql).singletonString();
    assertEquals(resultType, result);
  }

  @Test
  public void testModeOf() throws RpcException {

    // CSV files with headers use REQUIRED mode

    String sql = "SELECT modeOf(`name`) FROM cp.`store/text/data/cars.csvh`";
    String result = client.queryBuilder().sql(sql).singletonString();
    assertEquals("NOT NULL", result);

    // CSV files without headers use REPEATED mode

    sql = "SELECT modeOf(`columns`) FROM cp.`textinput/input2.csv`";
    result = client.queryBuilder().sql(sql).singletonString();
    assertEquals("ARRAY", result);

    // JSON files use OPTIONAL mode

    sql = "SELECT modeOf(`name`) FROM cp.`jsoninput/specialchar.json`";
    result = client.queryBuilder().sql(sql).singletonString();
    assertEquals("NULLABLE", result);
  }
}
