/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.drill.exec.vector.complex.writer;

import org.apache.commons.io.FileUtils;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.VarCharVector;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.apache.drill.test.TestBuilder.mapOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class TestJsonNanInf extends BaseTestQuery {


  @Test
  public void testNanInfSelect() throws Exception {
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
        .build()
        .run();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testExcludePositiveInfinity() throws Exception {
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
        .build()
        .run();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testExcludeNegativeInfinity() throws Exception {
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
        .build()
        .run();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testIncludePositiveInfinity() throws Exception {
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
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }


  @Test
  public void testIncludeNan() throws Exception {
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
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testNanInfFailure() throws Exception {
    String table = "nan_test.json";
    File file = new File(dirTestWatcher.getRootDir(), table);
    test("alter session set `%s` = false", ExecConstants.JSON_READER_NAN_INF_NUMBERS);
    String json = "{\"nan_col\":NaN, \"inf_col\":Infinity}";
    try {
      FileUtils.writeStringToFile(file, json);
      test("select * from dfs.`%s`;", table);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString("Error parsing JSON"));
      throw e;
    } finally {
      test("alter session reset `%s`", ExecConstants.JSON_READER_NAN_INF_NUMBERS);
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateTableNanInf() throws Exception {
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
      assertFalse("`NaN` must not be enclosed with \"\" ", resultJson.charAt(nanIndex - 1) == '"');
      assertFalse("`NaN` must not be enclosed with \"\" ", resultJson.charAt(nanIndex + "NaN".length()) == '"');
      int infIndex = resultJson.indexOf("Infinity");
      assertFalse("`Infinity` must not be enclosed with \"\" ", resultJson.charAt(infIndex - 1) == '"');
      assertFalse("`Infinity` must not be enclosed with \"\" ", resultJson.charAt(infIndex + "Infinity".length()) == '"');
    } finally {
      test("drop table if exists dfs.`%s`", newTable);
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testConvertFromJsonFunction() throws Exception {
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
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }



  @Test
  public void testConvertToJsonFunction() throws Exception {
    String table = "nan_test.csv";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String csv = "col_0, {\"nan_col\":NaN}";
    String query = String.format("select string_binary(convert_toJSON(convert_fromJSON(columns[1]))) as col " +
        "from dfs.`%s` where columns[0]='col_0'", table);
    try {
      FileUtils.writeStringToFile(file, csv);
      List<QueryDataBatch> results = testSqlWithResults(query);
      RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
      assertTrue("Query result must contain 1 row", results.size() == 1);
      QueryDataBatch batch = results.get(0);

      batchLoader.load(batch.getHeader().getDef(), batch.getData());
      VectorWrapper<?> vw = batchLoader.getValueAccessorById(VarCharVector.class, batchLoader.getValueVectorId(SchemaPath.getCompoundPath("col")).getFieldIds());
      // ensuring that `NaN` token ARE NOT enclosed with double quotes
      String resultJson = vw.getValueVector().getAccessor().getObject(0).toString();
      int nanIndex = resultJson.indexOf("NaN");
      assertFalse("`NaN` must not be enclosed with \"\" ", resultJson.charAt(nanIndex - 1) == '"');
      assertFalse("`NaN` must not be enclosed with \"\" ", resultJson.charAt(nanIndex + "NaN".length()) == '"');
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
          .build()
          .run();
  }

}
