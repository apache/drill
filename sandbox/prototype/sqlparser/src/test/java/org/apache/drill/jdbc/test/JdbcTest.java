/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.jdbc.test;

import junit.framework.TestCase;

import java.sql.*;

/** Unit tests for Drill's JDBC driver. */
public class JdbcTest extends TestCase {
  private static final String EXPECTED =
      "_MAP={donuts={batters={batter=[{id=1001, type=Regular}, {id=1002, type=Chocolate}, {id=1003, type=Blueberry}, {id=1004, type=Devil's Food}]}, id=0001, name=Cake, ppu=0.55, sales=35, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5005, type=Sugar}, {id=5007, type=Powdered Sugar}, {id=5006, type=Chocolate with Sprinkles}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}}\n"
      + "_MAP={donuts={batters={batter=[{id=1001, type=Regular}]}, id=0002, name=Raised, ppu=0.69, sales=145, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5005, type=Sugar}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}}\n"
      + "_MAP={donuts={batters={batter=[{id=1001, type=Regular}, {id=1002, type=Chocolate}]}, id=0003, name=Old Fashioned, ppu=0.55, sales=300, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}}\n"
      + "_MAP={donuts={batters={batter=[{id=1001, type=Regular}, {id=1002, type=Chocolate}, {id=1003, type=Blueberry}, {id=1004, type=Devil's Food}]}, filling=[{id=6001, type=None}, {id=6002, type=Raspberry}, {id=6003, type=Lemon}, {id=6004, type=Chocolate}, {id=6005, type=Kreme}], id=0004, name=Filled, ppu=0.69, sales=14, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5005, type=Sugar}, {id=5007, type=Powdered Sugar}, {id=5006, type=Chocolate with Sprinkles}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}}\n"
      + "_MAP={donuts={batters={batter=[{id=1001, type=Regular}]}, id=0005, name=Apple Fritter, ppu=1.0, sales=700, topping=[{id=5002, type=Glazed}], type=donut}}\n";

  /** Load driver. */
  public void testLoadDriver() throws ClassNotFoundException {
    Class.forName("org.apache.drill.jdbc.Driver");
  }

  /** Load driver and make a connection. */
  public void testConnect() throws Exception {
    Class.forName("org.apache.drill.jdbc.Driver");
    final Connection connection = DriverManager.getConnection(
        "jdbc:drill:schema=DONUTS;tables=EMP,DEPT");
    connection.close();
  }

  /** Load driver, make a connection, prepare a statement. */
  public void _testPrepare() throws Exception {
    Class.forName("org.apache.drill.jdbc.Driver");
    final Connection connection = DriverManager.getConnection(
        "jdbc:drill:schema=DONUTS;tables=EMP,DEPT");
    final Statement statement = connection.prepareStatement(
        "select * from emp where cast(name as varchar(10)) = 'Eric'");
    statement.close();
    connection.close();
  }

  /** Simple query against JSON. */
  public void testSelectJson() throws Exception {
    Class.forName("org.apache.drill.jdbc.Driver");
    final Connection connection = DriverManager.getConnection(
        "jdbc:drill:schema=DONUTS;tables=DONUTS");
    final Statement statement = connection.createStatement();
    final ResultSet resultSet = statement.executeQuery(
        "select * from donuts");
    assertEquals(
        EXPECTED,
        toString(resultSet));
    resultSet.close();
    statement.close();
    connection.close();
  }

  /** Query with project list. No field references yet. */
  public void testProjectConstant() throws Exception {
    assertSqlReturns(
        "select 1 + 3 as c from donuts",
        "C=4\n"
        + "C=4\n"
        + "C=4\n"
        + "C=4\n"
        + "C=4\n");
  }

  /** Query that projects an element from the map. */
  public void testProject() throws Exception {
    assertSqlReturns(
        "select _MAP['donuts']['ppu'] as ppu from donuts",
        "PPU=0.55\n"
        + "PPU=0.69\n"
        + "PPU=0.55\n"
        + "PPU=0.69\n"
        + "PPU=1.0\n");
  }

  /** Query with subquery, filter, and projection of one real and one
   * nonexistent field from a map field. */
  public void testProjectFilterSubquery() throws Exception {
    assertSqlReturns(
        "select d['name'] as name, d['xx'] as xx from (\n"
        + " select _MAP['donuts'] as d from donuts)\n"
        + "where cast(d['ppu'] as double) > 0.6",
        "NAME=Raised; XX=null\n"
        + "NAME=Filled; XX=null\n"
        + "NAME=Apple Fritter; XX=null\n");
  }

  /** Query that projects one field. (Disabled; uses sugared syntax.) */
  public void _testProjectNestedFieldSugared() throws Exception {
    assertSqlReturns(
        "select donuts.ppu from donuts",
        "C=4\n"
        + "C=4\n"
        + "C=4\n"
        + "C=4\n"
        + "C=4\n");
  }

  /** Query with filter. No field references yet. */
  public void testFilterConstant() throws Exception {
    assertSqlReturns(
        "select * from donuts where 3 > 4",
        "");
    assertSqlReturns(
        "select * from donuts where 3 < 4",
        EXPECTED);
  }

  private void assertSqlReturns(String sql,
      String expected) throws ClassNotFoundException, SQLException
  {
    Class.forName("org.apache.drill.jdbc.Driver");
    final Connection connection = DriverManager.getConnection(
        "jdbc:drill:schema=DONUTS;tables=DONUTS");
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertEquals(expected, toString(resultSet));
    } finally {
      connection.close();
    }
  }

  static String toString(ResultSet resultSet) throws SQLException {
    StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= n; i++) {
        buf.append(sep)
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
        sep = "; ";
      }
      buf.append("\n");
    }
    return buf.toString();
  }
}

// End JdbcTest.java
