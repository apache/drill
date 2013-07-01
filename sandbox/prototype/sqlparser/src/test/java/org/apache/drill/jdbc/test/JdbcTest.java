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

import com.google.common.base.Function;

import junit.framework.TestCase;

import org.apache.drill.jdbc.DrillTable;

import java.sql.*;

/** Unit tests for Drill's JDBC driver. */
public class JdbcTest extends TestCase {
  private static final String MODEL =
      "{\n"
      + "  version: '1.0',\n"
      + "   schemas: [\n"
      + "     {\n"
      + "       name: 'DONUTS',\n"
      + "       tables: [\n"
      + "         {\n"
      + "           name: 'DONUTS',\n"
      + "           type: 'custom',\n"
      + "           factory: '" + DrillTable.Factory.class.getName() + "'\n,"
      + "           operand: {\n"
      + "             path: '/donuts.json'\n"
      + "           }\n"
      + "         }\n"
      + "       ]\n"
      + "     }\n"
      + "   ]\n"
      + "}";

  private static final String EXPECTED =
      "_MAP={batters={batter=[{id=1001, type=Regular}, {id=1002, type=Chocolate}, {id=1003, type=Blueberry}, {id=1004, type=Devil's Food}]}, id=0001, name=Cake, ppu=0.55, sales=35, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5005, type=Sugar}, {id=5007, type=Powdered Sugar}, {id=5006, type=Chocolate with Sprinkles}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}\n"
      + "_MAP={batters={batter=[{id=1001, type=Regular}]}, id=0002, name=Raised, ppu=0.69, sales=145, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5005, type=Sugar}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}\n"
      + "_MAP={batters={batter=[{id=1001, type=Regular}, {id=1002, type=Chocolate}]}, id=0003, name=Old Fashioned, ppu=0.55, sales=300, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}\n"
      + "_MAP={batters={batter=[{id=1001, type=Regular}, {id=1002, type=Chocolate}, {id=1003, type=Blueberry}, {id=1004, type=Devil's Food}]}, filling=[{id=6001, type=None}, {id=6002, type=Raspberry}, {id=6003, type=Lemon}, {id=6004, type=Chocolate}, {id=6005, type=Kreme}], id=0004, name=Filled, ppu=0.69, sales=14, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5005, type=Sugar}, {id=5007, type=Powdered Sugar}, {id=5006, type=Chocolate with Sprinkles}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}\n"
      + "_MAP={batters={batter=[{id=1001, type=Regular}]}, id=0005, name=Apple Fritter, ppu=1.0, sales=700, topping=[{id=5002, type=Glazed}], type=donut}\n";

  /** Load driver. */
  public void testLoadDriver() throws ClassNotFoundException {
    Class.forName("org.apache.drill.jdbc.Driver");
  }

  /** Load driver and make a connection. */
  public void testConnect() throws Exception {
    Class.forName("org.apache.drill.jdbc.Driver");
    final Connection connection = DriverManager.getConnection(
        "jdbc:drill:schema=DONUTS");
    connection.close();
  }

  /** Load driver, make a connection, prepare a statement. */
  public void testPrepare() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .withConnection(
            new Function<Connection, Void>() {
              public Void apply(Connection connection) {
                try {
                  final Statement statement = connection.prepareStatement(
                      "select * from donuts");
                  statement.close();
                  return null;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Simple query against JSON. */
  public void testSelectJson() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select * from donuts")
        .returns(EXPECTED);
  }

  /** Query with project list. No field references yet. */
  public void testProjectConstant() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select 1 + 3 as c from donuts")
        .returns("C=4\n"
            + "C=4\n"
            + "C=4\n"
            + "C=4\n"
            + "C=4\n");
  }

  /** Query that projects an element from the map. */
  public void testProject() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select _MAP['ppu'] as ppu from donuts")
        .returns("PPU=0.55\n"
            + "PPU=0.69\n"
            + "PPU=0.55\n"
            + "PPU=0.69\n"
            + "PPU=1.0\n");
  }

  /** Same logic as {@link #testProject()}, but using a subquery. */
  public void testProjectOnSubquery() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select d['ppu'] as ppu from (\n"
             + " select _MAP as d from donuts)")
        .returns("PPU=0.55\n"
            + "PPU=0.69\n"
            + "PPU=0.55\n"
            + "PPU=0.69\n"
            + "PPU=1.0\n");
  }

  /** Checks the logical plan. */
  public void testProjectPlan() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select _MAP['ppu'] as ppu from donuts")
        .planContains(
            "{'head':{'type':'apache_drill_logical_plan','version':'1','generator':{'type':'manual','info':'na'}},"
            + "'storage':[{'name':'donuts-json','type':'classpath'},{'name':'queue','type':'queue'}],"
            + "'query':["
            + "{'op':'sequence','do':["
            + "{'op':'scan','memo':'initial_scan','ref':'_MAP','storageengine':'donuts-json','selection':{'path':'/donuts.json','type':'JSON'}},"
            + "{'op':'project','projections':[{'expr':'_MAP.ppu','ref':'output.PPU'}]},"
            + "{'op':'store','storageengine':'queue','memo':'output sink','target':{'number':0}}]}]}");
  }

  /** Query with subquery, filter, and projection of one real and one
   * nonexistent field from a map field. */
  public void testProjectFilterSubquery() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select d['name'] as name, d['xx'] as xx from (\n"
            + " select _MAP as d from donuts)\n"
            + "where cast(d['ppu'] as double) > 0.6")
        .returns("NAME=Raised; XX=null\n"
            + "NAME=Filled; XX=null\n"
            + "NAME=Apple Fritter; XX=null\n");
  }

  public void testProjectFilterSubqueryPlan() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select d['name'] as name, d['xx'] as xx from (\n"
            + " select _MAP['donuts'] as d from donuts)\n"
            + "where cast(d['ppu'] as double) > 0.6")
        .planContains(
            "{'head':{'type':'apache_drill_logical_plan','version':'1','generator':{'type':'manual','info':'na'}},'storage':[{'name':'donuts-json','type':'classpath'},{'name':'queue','type':'queue'}],"
            + "'query':["
            + "{'op':'sequence','do':["
            + "{'op':'scan','memo':'initial_scan','ref':'_MAP','storageengine':'donuts-json','selection':{'path':'/donuts.json','type':'JSON'}},"
            + "{'op':'filter','expr':'(_MAP.donuts.ppu > 0.6)'},"
            + "{'op':'project','projections':[{'expr':'_MAP.donuts','ref':'output.D'}]},"
            + "{'op':'project','projections':[{'expr':'D.name','ref':'output.NAME'},{'expr':'D.xx','ref':'output.XX'}]},"
            + "{'op':'store','storageengine':'queue','memo':'output sink','target':{'number':0}}]}]}");
  }

  /** Query that projects one field. (Disabled; uses sugared syntax.) */
  public void _testProjectNestedFieldSugared() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select donuts.ppu from donuts")
        .returns("C=4\n"
            + "C=4\n"
            + "C=4\n"
            + "C=4\n"
            + "C=4\n");
  }

  /** Query with filter. No field references yet. */
  public void testFilterConstantFalse() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select * from donuts where 3 > 4")
        .returns("");
  }

  public void testFilterConstant() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select * from donuts where 3 < 4")
        .returns(EXPECTED);
  }

  public void testValues() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("values (1)")
        .returns("EXPR$0=1\n");

    // Enable when https://issues.apache.org/jira/browse/DRILL-57 fixed
    // .planContains("store");
  }
}

// End JdbcTest.java
