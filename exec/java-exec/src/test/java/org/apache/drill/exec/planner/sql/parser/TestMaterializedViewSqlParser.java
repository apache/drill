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
package org.apache.drill.exec.planner.sql.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.parser.impl.DrillParserImpl;
import org.apache.drill.test.BaseTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for parsing materialized view SQL statements.
 */
@Category(SqlTest.class)
public class TestMaterializedViewSqlParser extends BaseTest {

  private SqlParser.Config parserConfig() {
    return SqlParser.config()
        .withParserFactory(DrillParserImpl.FACTORY)
        .withQuoting(Quoting.BACK_TICK)
        .withIdentifierMaxLength(PlannerSettings.DEFAULT_IDENTIFIER_MAX_LENGTH);
  }

  private SqlNode parse(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql, parserConfig());
    return parser.parseStmt();
  }

  @Test
  public void testParseCreateMaterializedView() throws SqlParseException {
    String sql = "CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t1";
    SqlNode node = parse(sql);

    assertNotNull(node);
    assertTrue(node instanceof SqlCreateMaterializedView);
    SqlCreateMaterializedView mv = (SqlCreateMaterializedView) node;
    assertEquals("MV1", mv.getName());
    assertEquals(SqlCreateType.SIMPLE, mv.getSqlCreateType());
  }

  @Test
  public void testParseCreateOrReplaceMaterializedView() throws SqlParseException {
    String sql = "CREATE OR REPLACE MATERIALIZED VIEW mv1 AS SELECT * FROM t1";
    SqlNode node = parse(sql);

    assertNotNull(node);
    assertTrue(node instanceof SqlCreateMaterializedView);
    SqlCreateMaterializedView mv = (SqlCreateMaterializedView) node;
    assertEquals("MV1", mv.getName());
    assertEquals(SqlCreateType.OR_REPLACE, mv.getSqlCreateType());
  }

  @Test
  public void testParseCreateMaterializedViewIfNotExists() throws SqlParseException {
    String sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv1 AS SELECT * FROM t1";
    SqlNode node = parse(sql);

    assertNotNull(node);
    assertTrue(node instanceof SqlCreateMaterializedView);
    SqlCreateMaterializedView mv = (SqlCreateMaterializedView) node;
    assertEquals("MV1", mv.getName());
    assertEquals(SqlCreateType.IF_NOT_EXISTS, mv.getSqlCreateType());
  }

  @Test
  public void testParseCreateMaterializedViewWithSchema() throws SqlParseException {
    String sql = "CREATE MATERIALIZED VIEW dfs.tmp.mv1 AS SELECT * FROM t1";
    SqlNode node = parse(sql);

    assertNotNull(node);
    assertTrue(node instanceof SqlCreateMaterializedView);
    SqlCreateMaterializedView mv = (SqlCreateMaterializedView) node;
    assertEquals("MV1", mv.getName());
    assertEquals(2, mv.getSchemaPath().size());
    assertEquals("DFS", mv.getSchemaPath().get(0));
    assertEquals("TMP", mv.getSchemaPath().get(1));
  }

  @Test
  public void testParseCreateMaterializedViewWithFieldList() throws SqlParseException {
    String sql = "CREATE MATERIALIZED VIEW mv1 (col1, col2) AS SELECT a, b FROM t1";
    SqlNode node = parse(sql);

    assertNotNull(node);
    assertTrue(node instanceof SqlCreateMaterializedView);
    SqlCreateMaterializedView mv = (SqlCreateMaterializedView) node;
    assertEquals("MV1", mv.getName());
    assertEquals(2, mv.getFieldNames().size());
    assertEquals("COL1", mv.getFieldNames().get(0));
    assertEquals("COL2", mv.getFieldNames().get(1));
  }

  @Test
  public void testParseDropMaterializedView() throws SqlParseException {
    String sql = "DROP MATERIALIZED VIEW mv1";
    SqlNode node = parse(sql);

    assertNotNull(node);
    assertTrue(node instanceof SqlDropMaterializedView);
    SqlDropMaterializedView drop = (SqlDropMaterializedView) node;
    assertEquals("MV1", drop.getName());
    assertEquals(false, drop.checkViewExistence());
  }

  @Test
  public void testParseDropMaterializedViewIfExists() throws SqlParseException {
    String sql = "DROP MATERIALIZED VIEW IF EXISTS mv1";
    SqlNode node = parse(sql);

    assertNotNull(node);
    assertTrue(node instanceof SqlDropMaterializedView);
    SqlDropMaterializedView drop = (SqlDropMaterializedView) node;
    assertEquals("MV1", drop.getName());
    assertEquals(true, drop.checkViewExistence());
  }

  @Test
  public void testParseDropMaterializedViewWithSchema() throws SqlParseException {
    String sql = "DROP MATERIALIZED VIEW dfs.tmp.mv1";
    SqlNode node = parse(sql);

    assertNotNull(node);
    assertTrue(node instanceof SqlDropMaterializedView);
    SqlDropMaterializedView drop = (SqlDropMaterializedView) node;
    assertEquals("MV1", drop.getName());
    assertEquals(2, drop.getSchemaPath().size());
  }

  @Test
  public void testParseRefreshMaterializedView() throws SqlParseException {
    String sql = "REFRESH MATERIALIZED VIEW mv1";
    SqlNode node = parse(sql);

    assertNotNull(node);
    assertTrue(node instanceof SqlRefreshMaterializedView);
    SqlRefreshMaterializedView refresh = (SqlRefreshMaterializedView) node;
    assertEquals("MV1", refresh.getName());
  }

  @Test
  public void testParseRefreshMaterializedViewWithSchema() throws SqlParseException {
    String sql = "REFRESH MATERIALIZED VIEW dfs.tmp.mv1";
    SqlNode node = parse(sql);

    assertNotNull(node);
    assertTrue(node instanceof SqlRefreshMaterializedView);
    SqlRefreshMaterializedView refresh = (SqlRefreshMaterializedView) node;
    assertEquals("MV1", refresh.getName());
    assertEquals(2, refresh.getSchemaPath().size());
  }

  @Test(expected = SqlParseException.class)
  public void testInvalidCreateMaterializedViewSyntax() throws SqlParseException {
    // Missing AS keyword
    String sql = "CREATE MATERIALIZED VIEW mv1 SELECT * FROM t1";
    parse(sql);
  }

  @Test(expected = SqlParseException.class)
  public void testInvalidCreateOrReplaceIfNotExists() throws SqlParseException {
    // Cannot have both OR REPLACE and IF NOT EXISTS
    String sql = "CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS mv1 AS SELECT * FROM t1";
    parse(sql);
  }

  @Test
  public void testUnparseCreateMaterializedView() throws SqlParseException {
    String sql = "CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t1";
    SqlNode node = parse(sql);
    String unparsed = node.toSqlString(null, true).getSql();

    assertTrue(unparsed.contains("CREATE"));
    assertTrue(unparsed.contains("MATERIALIZED"));
    assertTrue(unparsed.contains("VIEW"));
    assertTrue(unparsed.contains("MV1") || unparsed.contains("`MV1`"));
  }

  @Test
  public void testUnparseDropMaterializedView() throws SqlParseException {
    String sql = "DROP MATERIALIZED VIEW IF EXISTS mv1";
    SqlNode node = parse(sql);
    String unparsed = node.toSqlString(null, true).getSql();

    assertTrue(unparsed.contains("DROP"));
    assertTrue(unparsed.contains("MATERIALIZED"));
    assertTrue(unparsed.contains("VIEW"));
    assertTrue(unparsed.contains("IF"));
    assertTrue(unparsed.contains("EXISTS"));
  }

  @Test
  public void testUnparseRefreshMaterializedView() throws SqlParseException {
    String sql = "REFRESH MATERIALIZED VIEW mv1";
    SqlNode node = parse(sql);
    String unparsed = node.toSqlString(null, true).getSql();

    assertTrue(unparsed.contains("REFRESH"));
    assertTrue(unparsed.contains("MATERIALIZED"));
    assertTrue(unparsed.contains("VIEW"));
  }
}
