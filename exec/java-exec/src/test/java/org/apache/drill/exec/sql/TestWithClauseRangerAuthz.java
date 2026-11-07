/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.sql;

import org.apache.drill.categories.SqlTest;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * CTE (Common Table Expression, {@code WITH ... AS (...)}) authorization tests
 * under Ranger column-level access control.
 *
 * <p><b>Prerequisites:</b> These tests require a live Drill cluster with:
 * <ul>
 *   <li>Ranger authorization enabled ({@code drill.exec.security.authorizer.enabled=true})</li>
 *   <li>A MySQL storage plugin named {@code mysql} with schema {@code shf}</li>
 *   <li>Tables {@code mysql.shf.orders} (columns: id, amount, user_id, order_date)
 *       and {@code mysql.shf.users} (all columns)</li>
 *   <li>Ranger Policy A: {@code users} table, column {@code *}, SELECT</li>
 *   <li>Ranger Policy B: {@code orders} table, columns {@code id, amount}, SELECT</li>
 * </ul>
 * See {@code docs/dev/RangerAuthorization.md} section 4.1 for the sample policies.</p>
 *
 * <p><b>Why CTEs don't need special handling:</b> Calcite's
 * {@code SqlToRelConverter} inlines CTE definitions into the RelNode tree
 * before {@code ColumnAccessChecker} runs. After inlining, every
 * {@code TableScan} seen by the {@code RelShuttle} is a real underlying table,
 * so column-level checks apply uniformly regardless of whether the original
 * SQL used a CTE or a direct {@code SELECT}.</p>
 *
 * <p>Tests are {@code @Ignore}d by default because they depend on external
 * resources. Remove {@code @Ignore} when running against a configured
 * Ranger + MySQL environment.</p>
 */
@Category(SqlTest.class)
@Ignore("Requires Ranger authorization enabled with MySQL storage plugin and sample policies")
public class TestWithClauseRangerAuthz extends BaseTestQuery {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestWithClauseRangerAuthz.class);

  private static final String ACCESS_DENIED = UserBitShared.DrillPBError.ErrorType.PERMISSION.name();

  /**
   * DENY: CTE body uses {@code SELECT *} which expands to all columns including
   * {@code order_date} (not in Policy B).
   */
  @Test
  public void deny_cteSelectStarFromOrders() throws Exception {
    String query = "WITH t AS (SELECT * FROM mysql.shf.orders)\n" +
        "SELECT * FROM t";
    errorMsgTestHelper(query, ACCESS_DENIED);
  }

  /**
   * PASS: CTE body only selects {@code id} (in Policy B); outer query selects
   * from the CTE, which resolves to the same authorized column.
   */
  @Test
  public void pass_cteSelectAuthorizedColumn() throws Exception {
    String query = "WITH t AS (SELECT id FROM mysql.shf.orders)\n" +
        "SELECT * FROM t";
    test(query);
  }

  /**
   * DENY: CTE body selects {@code order_date} (not in Policy B). Even though
   * the outer query projects {@code order_date} from the CTE, the column
   * access check traces back to the underlying {@code orders} table scan.
   */
  @Test
  public void deny_cteSelectUnauthorizedColumnProjected() throws Exception {
    String query = "WITH t AS (SELECT id, order_date FROM mysql.shf.orders)\n" +
        "SELECT order_date FROM t";
    errorMsgTestHelper(query, ACCESS_DENIED);
  }

  /**
   * DENY: The CTE body references {@code order_date} (not in Policy B) even
   * though the outer query only projects {@code id}. After CTE inlining, the
   * {@code TableScan} for {@code orders} has both {@code id} and
   * {@code order_date} referenced, so the check fails on {@code order_date}.
   */
  @Test
  public void deny_cteBodyReferencesUnauthorizedColumnEvenIfOuterDoesNot() throws Exception {
    String query = "WITH t AS (SELECT id, order_date FROM mysql.shf.orders)\n" +
        "SELECT id FROM t";
    errorMsgTestHelper(query, ACCESS_DENIED);
  }

  /**
   * PASS: Multiple CTEs referencing different tables. CTE {@code a} selects
   * {@code id} from {@code orders} (Policy B); CTE {@code b} selects {@code id}
   * from {@code users} (Policy A via {@code *}). Outer query selects from
   * {@code b} only. All referenced columns are authorized.
   */
  @Test
  public void pass_multipleCtesDifferentTables() throws Exception {
    String query = "WITH a AS (SELECT id FROM mysql.shf.orders),\n" +
        "     b AS (SELECT id FROM mysql.shf.users)\n" +
        "SELECT * FROM b";
    test(query);
  }

  // ------------------------------------------------------------------
  // Table alias tests — verify that column-level authorization is
  // transparent to table aliases. Calcite resolves aliases during
  // SqlToRel conversion; RexInputRef indexes point to row-type
  // positions (not alias names), and RelMetadataQuery.getColumnOrigins
  // traces through to the underlying TableScan, so ColumnAccessChecker
  // sees the real table/column regardless of any alias used in SQL.
  // ------------------------------------------------------------------

  /**
   * DENY: Table alias on an unauthorized column. {@code o.order_date} resolves
   * to {@code order_date} of {@code orders} (not in Policy B).
   */
  @Test
  public void deny_simpleAliasUnauthorizedColumn() throws Exception {
    String query = "SELECT o.order_date FROM mysql.shf.orders o";
    errorMsgTestHelper(query, ACCESS_DENIED);
  }

  /**
   * PASS: Join with aliases on two tables. {@code a.id} resolves to
   * {@code users.id} (Policy A via {@code *}); {@code b.id} resolves to
   * {@code orders.id} (Policy B). Join condition columns also authorized.
   */
  @Test
  public void pass_joinWithAliases() throws Exception {
    String query = "SELECT a.id FROM mysql.shf.users a " +
        "JOIN mysql.shf.orders b ON a.id = b.id";
    test(query);
  }

  /**
   * PASS: Simple table alias on an authorized column. {@code o.id} resolves
   * to the {@code id} column of {@code orders} (in Policy B).
   */
  @Test
  public void pass_simpleAliasAuthorizedColumn() throws Exception {
    String query = "SELECT o.id FROM mysql.shf.orders o";
    test(query);
  }

  /**
   * PASS: Alias used in WHERE clause on an authorized column.
   * {@code o.amount} resolves to {@code orders.amount} (in Policy B).
   */
  @Test
  public void pass_aliasInWhereAuthorizedColumn() throws Exception {
    String query = "SELECT o.id FROM mysql.shf.orders o WHERE o.amount = '150.0'";
    test(query);
  }
}
