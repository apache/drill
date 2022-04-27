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
package org.apache.drill.exec.store.json;

import static org.apache.drill.test.TestBuilder.mapOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.join.JoinTestBase;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.store.json.TestJsonReader.TestWrapper;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Ignore;
import org.junit.Test;

// TODO: Split or rename: this tests mor than NanInf
public class TestJsonNanInf extends BaseTestQuery {

  public void runBoth(TestWrapper wrapper) throws Exception {
    try {
      enableV2Reader(false);
      wrapper.apply();
      enableV2Reader(true);
      wrapper.apply();
    } finally {
      resetV2Reader();
    }
  }

  @Test
  public void testNanInfSelect() throws Exception {
    runBoth(this::doTestNanInfSelect);
  }

  private void doTestNanInfSelect() throws Exception {
    String table = "nan_test.json";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String json = "{\"nan_col\":NaN, \"inf_col\":Infinity}";
    String query = String.format("select * from dfs.`%s`",table);
    try {
      FileUtils.writeStringToFile(file, json);
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("nan_col", "inf_col")
        .baselineValues(Double.NaN, Double.POSITIVE_INFINITY)
        .go();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  @Ignore // see DRILL-6018
  public void testExcludePositiveInfinity() throws Exception {
    runBoth(this::doTestExcludePositiveInfinity);
  }

  private void doTestExcludePositiveInfinity() throws Exception {
    String table = "nan_test.json";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String json = "[{\"nan_col\":NaN, \"inf_col\":Infinity}," +
        "{\"nan_col\":5.0, \"inf_col\":5.0}]";
    String query = String.format("select inf_col from dfs.`%s` where inf_col <> cast('Infinity' as double)",table);
    try {
      FileUtils.writeStringToFile(file, json);
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("inf_col")
        .baselineValues(5.0)
        .go();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  @Ignore // see DRILL-6018
  public void testExcludeNegativeInfinity() throws Exception {
    runBoth(this::doTestExcludeNegativeInfinity);
  }

  private void doTestExcludeNegativeInfinity() throws Exception {
    String table = "nan_test.json";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String json = "[{\"nan_col\":NaN, \"inf_col\":-Infinity}," +
        "{\"nan_col\":5.0, \"inf_col\":5.0}]";
    String query = String.format("select inf_col from dfs.`%s` where inf_col <> cast('-Infinity' as double)",table);
    try {
      FileUtils.writeStringToFile(file, json);
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("inf_col")
        .baselineValues(5.0)
        .go();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  @Ignore // see DRILL-6018
  public void testIncludePositiveInfinity() throws Exception {
    runBoth(this::doTestIncludePositiveInfinity);
  }

  private void doTestIncludePositiveInfinity() throws Exception {
    String table = "nan_test.json";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String json = "[{\"nan_col\":NaN, \"inf_col\":Infinity}," +
        "{\"nan_col\":5.0, \"inf_col\":5.0}]";
    String query = String.format("select inf_col from dfs.`%s` where inf_col = cast('Infinity' as double)",table);
    try {
      FileUtils.writeStringToFile(file, json);
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("inf_col")
          .baselineValues(Double.POSITIVE_INFINITY)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testExcludeNan() throws Exception {
    runBoth(this::doTestExcludeNan);
  }

  private void doTestExcludeNan() throws Exception {
    String table = "nan_test.json";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String json = "[{\"nan_col\":NaN, \"inf_col\":-Infinity}," +
        "{\"nan_col\":5.0, \"inf_col\":5.0}]";
    String query = String.format("select nan_col from dfs.`%s` where cast(nan_col as varchar) <> 'NaN'",table);
    try {
      FileUtils.writeStringToFile(file, json);
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("nan_col")
          .baselineValues(5.0)
          .go();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testIncludeNan() throws Exception {
    runBoth(this::doTestIncludeNan);
  }

  private void doTestIncludeNan() throws Exception {
    String table = "nan_test.json";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String json = "[{\"nan_col\":NaN, \"inf_col\":-Infinity}," +
        "{\"nan_col\":5.0, \"inf_col\":5.0}]";
    String query = String.format("select nan_col from dfs.`%s` where cast(nan_col as varchar) = 'NaN'",table);
    try {
      FileUtils.writeStringToFile(file, json);
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("nan_col")
          .baselineValues(Double.NaN)
          .go();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testNanInfFailure() throws Exception {
    runBoth(this::doTestNanInfFailure);
  }

  private void doTestNanInfFailure() throws Exception {
    String table = "nan_test.json";
    File file = new File(dirTestWatcher.getRootDir(), table);
    test("alter session set `%s` = false", ExecConstants.JSON_READER_NAN_INF_NUMBERS);
    String json = "{\"nan_col\":NaN, \"inf_col\":Infinity}";
    try {
      FileUtils.writeStringToFile(file, json);
      test("select * from dfs.`%s`;", table);
      fail();
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString("Error parsing JSON"));
    } finally {
      resetSessionOption(ExecConstants.JSON_READER_NAN_INF_NUMBERS);
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateTableNanInf() throws Exception {
    runBoth(this::doTestCreateTableNanInf);
  }

  private void doTestCreateTableNanInf() throws Exception {
    String table = "nan_test.json";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String json = "{\"nan_col\":NaN, \"inf_col\":Infinity}";
    String newTable = "ctas_test";
    try {
      FileUtils.writeStringToFile(file, json);
      test("alter session set `store.format`='json'");
      test("create table dfs.`%s` as select * from dfs.`%s`;", newTable, table);

      // ensuring that `NaN` and `Infinity` tokens ARE NOT enclosed with double quotes
      File resultFile = new File(new File(file.getParent(), newTable),"0_0_0.json");
      String resultJson = FileUtils.readFileToString(resultFile);
      int nanIndex = resultJson.indexOf("NaN");
      assertNotEquals("`NaN` must not be enclosed with \"\" ", '"', resultJson.charAt(nanIndex - 1));
      assertNotEquals("`NaN` must not be enclosed with \"\" ", '"', resultJson.charAt(nanIndex + "NaN".length()));
      int infIndex = resultJson.indexOf("Infinity");
      assertNotEquals("`Infinity` must not be enclosed with \"\" ", '"', resultJson.charAt(infIndex - 1));
      assertNotEquals("`Infinity` must not be enclosed with \"\" ", '"', resultJson.charAt(infIndex + "Infinity".length()));
    } finally {
      test("drop table if exists dfs.`%s`", newTable);
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testConvertFromJsonFunction() throws Exception {
    runBoth(this::doTestConvertFromJsonFunction);
  }

  private void doTestConvertFromJsonFunction() throws Exception {
    String table = "nan_test.csv";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String csv = "col_0, {\"nan_col\":NaN}";
    try {
      FileUtils.writeStringToFile(file, csv);
      testBuilder()
          .sqlQuery(String.format("select convert_fromJSON(columns[1]) as col from dfs.`%s`", table))
          .unOrdered()
          .baselineColumns("col")
          .baselineValues(mapOf("nan_col", Double.NaN))
          .go();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testLargeStringBinary() throws Exception {
    runBoth(() -> doTestLargeStringBinary());
  }

  private void doTestLargeStringBinary() throws Exception {
    String chunk = "0123456789";
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      builder.append(chunk);
    }
    String data = builder.toString();
    test("select string_binary(binary_string('%s')) from (values(1))", data);
 }

  @Test
  public void testConvertToJsonFunction() throws Exception {
    runBoth(() -> doTestConvertToJsonFunction());
  }

  private void doTestConvertToJsonFunction() throws Exception {
    String table = "nan_test.csv";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String csv = "col_0, {\"nan_col\":NaN}";
    String query = String.format("select string_binary(convert_toJSON(convert_fromJSON(columns[1]))) as col " +
        "from dfs.`%s` where columns[0]='col_0'", table);
    try {
      FileUtils.writeStringToFile(file, csv);
      List<QueryDataBatch> results = testSqlWithResults(query);
      RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
      assertEquals("Query result must contain 1 row", 1, results.size());
      QueryDataBatch batch = results.get(0);

      batchLoader.load(batch.getHeader().getDef(), batch.getData());
      VectorWrapper<?> vw = batchLoader.getValueAccessorById(VarCharVector.class, batchLoader.getValueVectorId(SchemaPath.getCompoundPath("col")).getFieldIds());
      // ensuring that `NaN` token ARE NOT enclosed with double quotes
      String resultJson = vw.getValueVector().getAccessor().getObject(0).toString();
      int nanIndex = resultJson.indexOf("NaN");
      assertNotEquals("`NaN` must not be enclosed with \"\" ", '"', resultJson.charAt(nanIndex - 1));
      assertNotEquals("`NaN` must not be enclosed with \"\" ", '"', resultJson.charAt(nanIndex + "NaN".length()));
      batch.release();
      batchLoader.clear();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  @Ignore("DRILL-6018")
  public void testNanInfLiterals() throws Exception {
      testBuilder()
          .sqlQuery("  select sin(cast('NaN' as double)) as sin_col, " +
              "cast('Infinity' as double)+1 as sum_col from (values(1))")
          .unOrdered()
          .baselineColumns("sin_col", "sum_col")
          .baselineValues(Double.NaN, Double.POSITIVE_INFINITY)
          .go();
  }

  @Test
  public void testOrderByWithNaN() throws Exception {
    runBoth(this::doTestOrderByWithNaN);
  }

  private void doTestOrderByWithNaN() throws Exception {
    String table_name = "nan_test.json";
    String json = "{\"name\":\"obj1\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}\n" +
        "{\"name\":\"obj1\", \"attr1\":1, \"attr2\":2, \"attr3\":4, \"attr4\":Infinity}\n" +
        "{\"name\":\"obj2\", \"attr1\":1, \"attr2\":2, \"attr3\":5, \"attr4\":-Infinity}\n" +
        "{\"name\":\"obj2\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}";
    String query = String.format("SELECT name, attr4 from dfs.`%s` order by name, attr4 ", table_name);

    File file = new File(dirTestWatcher.getRootDir(), table_name);
    try {
      FileUtils.writeStringToFile(file, json);
      test("alter session set `%s` = true", ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
      testBuilder()
          .sqlQuery(query)
          .ordered()
          .baselineColumns("name", "attr4")
          .baselineValues("obj1", Double.POSITIVE_INFINITY)
          .baselineValues("obj1", Double.NaN)
          .baselineValues("obj2", Double.NEGATIVE_INFINITY)
          .baselineValues("obj2", Double.NaN)
          .go();
    } finally {
      test("alter session set `%s` = false", ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testNestedLoopJoinWithNaN() throws Exception {
    runBoth(this::doTestNestedLoopJoinWithNaN);
  }

  private void doTestNestedLoopJoinWithNaN() throws Exception {
    String table_name = "nan_test.json";
    String json = "{\"name\":\"object1\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}\n" +
            "{\"name\":\"object1\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}\n" +
            "{\"name\":\"object1\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}\n" +
            "{\"name\":\"object1\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}\n" +
            "{\"name\":\"object2\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":Infinity}\n" +
            "{\"name\":\"object2\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":Infinity}\n" +
            "{\"name\":\"object3\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":Infinity}\n" +
            "{\"name\":\"object3\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":Infinity}\n" +
            "{\"name\":\"object4\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}\n" +
            "{\"name\":\"object4\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}\n" +
            "{\"name\":\"object4\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":Infinity}";
    JoinTestBase.enableJoin(false, false, true);
    String query = String.format("select distinct t.name from dfs.`%s` t inner join dfs.`%s` " +
        " tt on t.attr4 = tt.attr4 ", table_name, table_name);

    File file = new File(dirTestWatcher.getRootDir(), table_name);
    try {
      FileUtils.writeStringToFile(file, json);
      test("alter session set `%s` = true", ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
      testBuilder()
          .sqlQuery(query)
          .ordered()
          .baselineColumns("name")
          .baselineValues("object1")
          .baselineValues("object2")
          .baselineValues("object3")
          .baselineValues("object4")
          .go();
    } finally {
      test("alter session set `%s` = false", ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
      JoinTestBase.resetJoinOptions();
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testHashJoinWithNaN() throws Exception {
    runBoth(this::doTestHashJoinWithNaN);
  }

  private void doTestHashJoinWithNaN() throws Exception {
    String table_name = "nan_test.json";
    String json = "{\"name\":\"obj1\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}\n" +
            "{\"name\":\"obj1\", \"attr1\":1, \"attr2\":2, \"attr3\":4, \"attr4\":Infinity}\n" +
            "{\"name\":\"obj2\", \"attr1\":1, \"attr2\":2, \"attr3\":5, \"attr4\":-Infinity}\n" +
            "{\"name\":\"obj2\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}";
    JoinTestBase.enableJoin(true, false, false);
    String query = String.format("select distinct t.name from dfs.`%s` t inner join dfs.`%s` " +
            " tt on t.attr4 = tt.attr4 ", table_name, table_name);

    File file = new File(dirTestWatcher.getRootDir(), table_name);
    try {
      FileUtils.writeStringToFile(file, json);
      test("alter session set `%s` = true", ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
      testBuilder()
          .sqlQuery(query)
          .ordered()
          .baselineColumns("name")
          .baselineValues("obj1")
          .baselineValues("obj2")
          .go();
    } finally {
      test("alter session set `%s` = false", ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
      JoinTestBase.resetJoinOptions();
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testMergeJoinWithNaN() throws Exception {
    runBoth(this::doTestMergeJoinWithNaN);
  }

  private void doTestMergeJoinWithNaN() throws Exception {
    String table_name = "nan_test.json";
    String json = "{\"name\":\"obj1\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}\n" +
            "{\"name\":\"obj1\", \"attr1\":1, \"attr2\":2, \"attr3\":4, \"attr4\":Infinity}\n" +
            "{\"name\":\"obj2\", \"attr1\":1, \"attr2\":2, \"attr3\":5, \"attr4\":-Infinity}\n" +
            "{\"name\":\"obj2\", \"attr1\":1, \"attr2\":2, \"attr3\":3, \"attr4\":NaN}";
    JoinTestBase.enableJoin(false, true, false);
    String query = String.format("select distinct t.name from dfs.`%s` t inner join dfs.`%s` " +
            " tt on t.attr4 = tt.attr4 ", table_name, table_name);

    File file = new File(dirTestWatcher.getRootDir(), table_name);
    try {
      FileUtils.writeStringToFile(file, json);
      test("alter session set `%s` = true", ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
      testBuilder()
          .sqlQuery(query)
          .ordered()
          .baselineColumns("name")
          .baselineValues("obj1")
          .baselineValues("obj2")
          .go();
    } finally {
      test("alter session set `%s` = false", ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
      JoinTestBase.resetJoinOptions();
      FileUtils.deleteQuietly(file);
    }
  }

  private void enableV2Reader(boolean enable) throws Exception {
    alterSession(ExecConstants.ENABLE_V2_JSON_READER_KEY, enable);
  }

  private void resetV2Reader() throws Exception {
    resetSessionOption(ExecConstants.ENABLE_V2_JSON_READER_KEY);
  }
}
