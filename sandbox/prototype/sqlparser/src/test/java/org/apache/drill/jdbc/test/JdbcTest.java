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
        "_extra={\"donuts\":{\"type\":\"donut\",\"ppu\":0.55,\"topping\":[{\"type\":\"None\",\"id\":\"5001\"},{\"type\":\"Glazed\",\"id\":\"5002\"},{\"type\":\"Sugar\",\"id\":\"5005\"},{\"type\":\"Powdered Sugar\",\"id\":\"5007\"},{\"type\":\"Chocolate with Sprinkles\",\"id\":\"5006\"},{\"type\":\"Chocolate\",\"id\":\"5003\"},{\"type\":\"Maple\",\"id\":\"5004\"}],\"name\":\"Cake\",\"sales\":35,\"batters\":{\"batter\":[{\"type\":\"Regular\",\"id\":\"1001\"},{\"type\":\"Chocolate\",\"id\":\"1002\"},{\"type\":\"Blueberry\",\"id\":\"1003\"},{\"type\":\"Devil's Food\",\"id\":\"1004\"}]},\"id\":\"0001\"}}\n"
        + "_extra={\"donuts\":{\"type\":\"donut\",\"ppu\":0.69,\"topping\":[{\"type\":\"None\",\"id\":\"5001\"},{\"type\":\"Glazed\",\"id\":\"5002\"},{\"type\":\"Sugar\",\"id\":\"5005\"},{\"type\":\"Chocolate\",\"id\":\"5003\"},{\"type\":\"Maple\",\"id\":\"5004\"}],\"name\":\"Raised\",\"sales\":145,\"batters\":{\"batter\":[{\"type\":\"Regular\",\"id\":\"1001\"}]},\"id\":\"0002\"}}\n"
        + "_extra={\"donuts\":{\"type\":\"donut\",\"ppu\":0.55,\"topping\":[{\"type\":\"None\",\"id\":\"5001\"},{\"type\":\"Glazed\",\"id\":\"5002\"},{\"type\":\"Chocolate\",\"id\":\"5003\"},{\"type\":\"Maple\",\"id\":\"5004\"}],\"name\":\"Old Fashioned\",\"sales\":300,\"batters\":{\"batter\":[{\"type\":\"Regular\",\"id\":\"1001\"},{\"type\":\"Chocolate\",\"id\":\"1002\"}]},\"id\":\"0003\"}}\n"
        + "_extra={\"donuts\":{\"type\":\"donut\",\"ppu\":0.69,\"topping\":[{\"type\":\"None\",\"id\":\"5001\"},{\"type\":\"Glazed\",\"id\":\"5002\"},{\"type\":\"Sugar\",\"id\":\"5005\"},{\"type\":\"Powdered Sugar\",\"id\":\"5007\"},{\"type\":\"Chocolate with Sprinkles\",\"id\":\"5006\"},{\"type\":\"Chocolate\",\"id\":\"5003\"},{\"type\":\"Maple\",\"id\":\"5004\"}],\"name\":\"Filled\",\"sales\":14,\"batters\":{\"batter\":[{\"type\":\"Regular\",\"id\":\"1001\"},{\"type\":\"Chocolate\",\"id\":\"1002\"},{\"type\":\"Blueberry\",\"id\":\"1003\"},{\"type\":\"Devil's Food\",\"id\":\"1004\"}]},\"id\":\"0004\",\"filling\":[{\"type\":\"None\",\"id\":\"6001\"},{\"type\":\"Raspberry\",\"id\":\"6002\"},{\"type\":\"Lemon\",\"id\":\"6003\"},{\"type\":\"Chocolate\",\"id\":\"6004\"},{\"type\":\"Kreme\",\"id\":\"6005\"}]}}\n"
        + "_extra={\"donuts\":{\"type\":\"donut\",\"ppu\":1.0,\"topping\":[{\"type\":\"Glazed\",\"id\":\"5002\"}],\"name\":\"Apple Fritter\",\"sales\":700,\"batters\":{\"batter\":[{\"type\":\"Regular\",\"id\":\"1001\"}]},\"id\":\"0005\"}}\n",
        toString(resultSet));
    resultSet.close();
    statement.close();
    connection.close();
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
