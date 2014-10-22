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

import org.junit.Test;

public class TestHBaseFilterPushDown extends BaseHBaseTest {

  @Test
  public void testFilterPushDownRowKeyEqual() throws Exception {
    setColumnWidths(new int[] {8, 38, 38});
    runHBaseSQLVerifyCount("SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key = 'b4'"
        , 1);
  }

  @Test
  public void testFilterPushDownRowKeyLike() throws Exception {
    setColumnWidths(new int[] {8, 22});
    runHBaseSQLVerifyCount("SELECT\n"
        + "  row_key, convert_from(tableName.f.c, 'UTF8') `f.c`\n"
        + "FROM\n"
        + "  hbase.`TestTable3` tableName\n"
        + "WHERE\n"
        + "  row_key LIKE '08%0' OR row_key LIKE '%70'"
        , 21);
  }

  @Test
  public void testFilterPushDownRowKeyLikeWithEscape() throws Exception {
    setColumnWidths(new int[] {8, 22});
    runHBaseSQLVerifyCount("SELECT\n"
        + "  row_key, convert_from(tableName.f.c, 'UTF8') `f.c`\n"
        + "FROM\n"
        + "  hbase.`TestTable3` tableName\n"
        + "WHERE\n"
        + "  row_key LIKE '!%!_AS!_PREFIX!_%' ESCAPE '!'"
        , 2);
  }

  @Test
  public void testFilterPushDownRowKeyRangeAndColumnValueLike() throws Exception {
    setColumnWidths(new int[] {8, 22});
    runHBaseSQLVerifyCount("SELECT\n"
        + "  row_key, convert_from(tableName.f.c, 'UTF8') `f.c`\n"
        + "FROM\n"
        + "  hbase.`TestTable3` tableName\n"
        + "WHERE\n"
        + " row_key >= '07' AND row_key < '09' AND tableName.f.c LIKE 'value 0%9'"
        , 22);
  }

  @Test
  public void testFilterPushDownRowKeyGreaterThan() throws Exception {
    setColumnWidths(new int[] {8, 38, 38});
    runHBaseSQLVerifyCount("SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key > 'b4'"
        , 2);
  }

  @Test
  public void testFilterPushDownRowKeyBetween() throws Exception {
    setColumnWidths(new int[] {8, 74, 38});
    runHBaseSQLVerifyCount("SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  row_key BETWEEN 'a2' AND 'b4'"
        , 3);
  }

  @Test
  public void testFilterPushDownMultiColumns() throws Exception {
    setColumnWidths(new int[] {8, 74, 38});
    runHBaseSQLVerifyCount("SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` t\n"
        + "WHERE\n"
        + "  (row_key >= 'b5' OR row_key <= 'a2') AND (t.f.c1 >= '1' OR t.f.c1 is null)"
        , 4);
  }

  @Test
  public void testFilterPushDownConvertExpression() throws Exception {
    setColumnWidths(new int[] {8, 38, 38});
    runHBaseSQLVerifyCount("SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  convert_from(row_key, 'UTF8') > 'b4'"
        , 2);
  }

  @Test
  public void testFilterPushDownRowKeyLessThanOrEqualTo() throws Exception {
    setColumnWidths(new int[] {8, 74, 38});
    runHBaseSQLVerifyCount("SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "WHERE\n"
        + "  'b4' >= row_key"
        , 4);
  }

}
