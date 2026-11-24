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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.exec.planner.sql.parser.impl.DrillParserImpl;
import org.apache.drill.test.BaseTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(SqlTest.class)
public class TestClearPlanCacheSqlParser extends BaseTest {
  private SqlNode parse(String sql) throws SqlParseException {
    return SqlParser.create(sql, SqlParser.config().withParserFactory(DrillParserImpl.FACTORY)).parseStmt();
  }

  @Test
  public void testClearPlanCacheRoundTrip() throws Exception {
    SqlNode node = parse("alter system clear plan cache");
    assertTrue(node instanceof SqlClearPlanCache);
    assertEquals(SqlKind.OTHER_DDL, node.getKind());
    assertEquals("SYSTEM", ((SqlClearPlanCache) node).getScope());
    assertTrue(parse(node.toString()) instanceof SqlClearPlanCache);
    assertTrue(SqlNode.clone(node) instanceof SqlClearPlanCache);
  }

  @Test
  public void testOnlySystemScopeIsAccepted() {
    assertThrows(SqlParseException.class, () -> parse("ALTER SESSION CLEAR PLAN CACHE"));
    assertThrows(SqlParseException.class, () -> parse("CLEAR PLAN CACHE"));
    assertThrows(SqlParseException.class, () -> parse("ALTER SYSTEM CLEAR PLAN"));
    assertThrows(SqlParseException.class, () -> parse("ALTER SYSTEM CLEAR PLAN CACHE extra"));
  }

  @Test
  public void testExistingOptionSyntaxAndIdentifiersStillParse() throws Exception {
    assertEquals(SqlKind.SET_OPTION, parse("ALTER SYSTEM SET foo = 1").getKind());
    assertTrue(parse("ALTER SYSTEM RESET ALL") instanceof DrillSqlResetOption);
    assertEquals(SqlKind.SET_OPTION, parse("ALTER SESSION SET foo = 1").getKind());
    assertTrue(parse("ALTER SESSION RESET foo") instanceof DrillSqlResetOption);
    assertEquals(SqlKind.SELECT, parse("SELECT clear, cache FROM t").getKind());
  }
}
