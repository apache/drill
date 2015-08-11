/**
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
package org.apache.drill.hbase;

import org.apache.drill.PlanTestBase;
import org.junit.Test;

public class TestHBaseFilterPushDown extends BaseHBaseTest {

  @Test
  public void testFilterPushDownRowKeyEqual() throws Exception {
    setColumnWidths(new int[] {8, 38, 38});
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key = 'b4'";

    runHBaseSQLVerifyCount(sql, 1);

    final String[] expectedPlan = {".*startRow=b4, stopRow=b4\\\\x00, filter=null.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownRowKeyEqualWithItem() throws Exception {
    setColumnWidths(new int[] {20, 30});
    final String sql = "SELECT\n"
        + "  cast(tableName.row_key as varchar(20)), cast(tableName.f.c1 as varchar(30))\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key = 'b4'";

    runHBaseSQLVerifyCount(sql, 1);

    final String[] expectedPlan = {".*startRow=b4, stopRow=b4\\\\x00, filter=null.*"};
    final String[] excludedPlan ={".*startRow=null, stopRow=null.*"};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownRowKeyLike() throws Exception {
    setColumnWidths(new int[] {8, 22});
    final String sql = "SELECT\n"
        + "  row_key, convert_from(tableName.f.c, 'UTF8') `f.c`\n"
        + "FROM\n"
        + "  hbase.`TestTable3` tableName\n"
        + "WHERE\n"
        + "  row_key LIKE '08%0' OR row_key LIKE '%70'";

    runHBaseSQLVerifyCount(sql, 21);

    final String[] expectedPlan = {".*filter=FilterList OR.*EQUAL.*EQUAL.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownRowKeyLikeWithEscape() throws Exception {
    setColumnWidths(new int[] {8, 22});
    final String sql = "SELECT\n"
        + "  row_key, convert_from(tableName.f.c, 'UTF8') `f.c`\n"
        + "FROM\n"
        + "  hbase.`TestTable3` tableName\n"
        + "WHERE\n"
        + "  row_key LIKE '!%!_AS!_PREFIX!_%' ESCAPE '!'";

    runHBaseSQLVerifyCount(sql, 2);

    final String[] expectedPlan = {".*startRow=\\%_AS_PREFIX_, stopRow=\\%_AS_PREFIX`, filter=RowFilter.*EQUAL.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownRowKeyRangeAndColumnValueLike() throws Exception {
    setColumnWidths(new int[] {8, 22});
    final String sql = "SELECT\n"
        + "  row_key, convert_from(tableName.f.c, 'UTF8') `f.c`\n"
        + "FROM\n"
        + "  hbase.`TestTable3` tableName\n"
        + "WHERE\n"
        + " row_key >= '07' AND row_key < '09' AND tableName.f.c LIKE 'value 0%9'";

    runHBaseSQLVerifyCount(sql, 22);

    final String[] expectedPlan = {".*startRow=07, stopRow=09, filter=FilterList AND.*RowFilter \\(GREATER_OR_EQUAL, 07\\), RowFilter \\(LESS, 09\\), SingleColumnValueFilter \\(f, c, EQUAL.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownRowKeyGreaterThan() throws Exception {
    setColumnWidths(new int[] {8, 38, 38});
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key > 'b4'";

    runHBaseSQLVerifyCount(sql, 3);

    final String[] expectedPlan = {".*startRow=b4\\\\x00.*stopRow=,.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownRowKeyGreaterThanWithItem() throws Exception {
    setColumnWidths(new int[] {8, 38});
    final String sql = "SELECT\n"
        + "  row_key, tableName.f2.c3\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key > 'b4'";

    runHBaseSQLVerifyCount(sql, 2);

    final String[] expectedPlan = {".*startRow=b4\\\\x00.*stopRow=, filter=null.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownRowKeyBetween() throws Exception {
    setColumnWidths(new int[] {8, 74, 38});
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key BETWEEN 'a2' AND 'b4'";

    runHBaseSQLVerifyCount(sql, 3);

    final String[] expectedPlan = {".*startRow=a2, stopRow=b4\\\\x00, filter=FilterList AND.*GREATER_OR_EQUAL, a2.*LESS_OR_EQUAL, b4.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownRowKeyBetweenWithItem() throws Exception {
    setColumnWidths(new int[] {8, 12});
    final String sql = "SELECT\n"
        + "  row_key, tableName.f.c1\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key BETWEEN 'a2' AND 'b4'";

    runHBaseSQLVerifyCount(sql, 3);

    final String[] expectedPlan = {".*startRow=a2, stopRow=b4\\\\x00, filter=FilterList AND.*GREATER_OR_EQUAL, a2.*LESS_OR_EQUAL, b4.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownMultiColumns() throws Exception {
    setColumnWidths(new int[] {8, 74, 38});
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` t\n"
        + "WHERE\n"
        + "  (row_key >= 'b5' OR row_key <= 'a2') AND (t.f.c1 >= '1' OR t.f.c1 is null)";

    runHBaseSQLVerifyCount(sql, 4);

    final String[] expectedPlan = {".*startRow=, stopRow=, filter=FilterList OR.*GREATER_OR_EQUAL, b5.*LESS_OR_EQUAL, a2.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownMultiColumnsWithItem() throws Exception {
    setColumnWidths(new int[] {8, 8});
    final String sql = "SELECT\n"
        + "  row_key, t.f.c1\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` t\n"
        + "WHERE\n"
        + "  (row_key >= 'b5' OR row_key <= 'a2') AND (t.f.c1 >= '1' OR t.f.c1 is null)";

    final String[] expectedPlan = {".*startRow=, stopRow=, filter=FilterList OR.*GREATER_OR_EQUAL, b5.*LESS_OR_EQUAL, a2.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownConvertExpression() throws Exception {
    setColumnWidths(new int[] {8, 38, 38});
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  convert_from(row_key, 'UTF8') > 'b4'";

    runHBaseSQLVerifyCount(sql, 3);

    final String[] expectedPlan = {".*startRow=b4\\\\x00, stopRow=,.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownConvertExpressionWithItem() throws Exception {
    setColumnWidths(new int[] {8, 38});
    final String sql = "SELECT\n"
        + "  row_key, tableName.f2.c3\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  convert_from(row_key, 'UTF8') > 'b4'";

    runHBaseSQLVerifyCount(sql, 2);

    final String[] expectedPlan = {".*startRow=b4\\\\x00, stopRow=,.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownConvertExpressionWithNumber() throws Exception {
    setColumnWidths(new int[] {8, 1100});
    runHBaseSQLVerifyCount("EXPLAIN PLAN FOR\n"
        + "SELECT\n"
        + "  row_key\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  convert_from(row_key, 'INT_BE') = 75"
        , 1);
  }

  @Test
  public void testFilterPushDownRowKeyLessThanOrEqualTo() throws Exception {
    setColumnWidths(new int[] {8, 74, 38});
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  'b4' >= row_key";

    runHBaseSQLVerifyCount(sql, 4);

    final String[] expectedPlan = {".*startRow=, stopRow=b4\\\\x00, filter=null.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownRowKeyLessThanOrEqualToWithItem() throws Exception {
    setColumnWidths(new int[] {8, 12});
    final String sql = "SELECT\n"
        + "  row_key, tableName.f.c1\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  'b4' >= row_key";

    runHBaseSQLVerifyCount(sql, 4);

    final String[] expectedPlan = {".*startRow=, stopRow=b4\\\\x00, filter=null.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);
  }

  @Test
  public void testFilterPushDownOrRowKeyEqual() throws Exception {
    setColumnWidths(new int[] {8, 38, 38});
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key = 'b4' or row_key = 'a2'";

    runHBaseSQLVerifyCount(sql, 2);

    final String[] expectedPlan = {".*startRow=a2, stopRow=b4\\\\x00, filter=FilterList OR \\(2/2\\): \\[RowFilter \\(EQUAL, b4\\), RowFilter \\(EQUAL, a2\\).*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);

  }

  @Test
  public void testFilterPushDownOrRowKeyInPred() throws Exception {
    setColumnWidths(new int[] {8, 38, 38});
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key in ('b4', 'a2')";

    runHBaseSQLVerifyCount(sql, 2);

    final String[] expectedPlan = {".*startRow=a2, stopRow=b4\\\\x00, filter=FilterList OR \\(2/2\\): \\[RowFilter \\(EQUAL, b4\\), RowFilter \\(EQUAL, a2\\).*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);

  }

  @Test
  public void testFilterPushDownOrRowKeyEqualRangePred() throws Exception {
    setColumnWidths(new int[] {8, 38, 38});
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key = 'a2' or row_key between 'b5' and 'b6'";

    runHBaseSQLVerifyCount(sql, 3);

    final String[] expectedPlan = {".*startRow=a2, stopRow=b6\\\\x00, filter=FilterList OR \\(2/2\\): \\[RowFilter \\(EQUAL, a2\\), FilterList AND \\(2/2\\): \\[RowFilter \\(GREATER_OR_EQUAL, b5\\), RowFilter \\(LESS_OR_EQUAL, b6.*"};
    final String[] excludedPlan ={};
    final String sqlHBase = canonizeHBaseSQL(sql);
    PlanTestBase.testPlanMatchingPatterns(sqlHBase, expectedPlan, excludedPlan);

  }

}

