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

import org.apache.commons.io.FileUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import static org.apache.drill.test.TestBuilder.mapOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestJsonConversionUDF extends BaseTestQuery {

  @Test
  public void doTestConvertFromJsonFunction() throws Exception {
    String table = "nan_test.csv";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String csv = "col_0, {\"nan_col\":NaN}";
    try {
      FileUtils.writeStringToFile(file, csv, Charset.defaultCharset());
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
  public void doTestConvertToJsonFunction() throws Exception {
    String table = "nan_test.csv";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String csv = "col_0, {\"nan_col\":NaN}";
    String query = String.format("select string_binary(convert_toJSON(convert_fromJSON(columns[1]))) as col " +
        "from dfs.`%s` where columns[0]='col_0'", table);
    try {
      FileUtils.writeStringToFile(file, csv, Charset.defaultCharset());
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
  public void testAllTextMode() throws Exception {
    alterSession(ExecConstants.JSON_ALL_TEXT_MODE, true);
    alterSession(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE, false);

    String sql = "SELECT \n" +
        "typeof(jsonMap['bi']) AS bi, \n" +
        "typeof(jsonMap['fl']) AS fl, \n" +
        "typeof(jsonMap['st']) AS st, \n" +
        "typeof(jsonMap['mp']) AS mp, \n" +
        "typeof(jsonMap['ar']) AS ar, \n" +
        "typeof(jsonMap['nu']) AS nu\n" +
        "FROM(\n" +
        "SELECT convert_fromJSON(col1) AS jsonMap, \n" +
        "col2 \n" +
        "FROM cp.`jsoninput/allTypes.csvh`\n" +
        ")";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("bi", "fl", "st", "mp", "ar", "nu")
        .baselineValues("VARCHAR", "VARCHAR", "VARCHAR", "MAP", "VARCHAR", "NULL")
        .go();

    alterSession(ExecConstants.JSON_ALL_TEXT_MODE, false);

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("bi", "fl", "st", "mp", "ar", "nu")
        .baselineValues("BIGINT", "FLOAT8", "VARCHAR", "MAP", "BIGINT", "NULL")
        .go();

    resetSessionOption(ExecConstants.JSON_ALL_TEXT_MODE);
    resetSessionOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
  }

  @Test
  public void testAllTextModeFromArgs() throws Exception {
    // Set the system options to make sure that the UDF is using the provided options rather than
    // the system options.
    alterSession(ExecConstants.JSON_ALL_TEXT_MODE, false);
    alterSession(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE, true);

    String sql = "SELECT \n" +
        "typeof(jsonMap['bi']) AS bi, \n" +
        "typeof(jsonMap['fl']) AS fl, \n" +
        "typeof(jsonMap['st']) AS st, \n" +
        "typeof(jsonMap['mp']) AS mp, \n" +
        "typeof(jsonMap['ar']) AS ar, \n" +
        "typeof(jsonMap['nu']) AS nu\n" +
        "FROM(\n" +
        "SELECT convert_fromJSON(col1, true, false) AS jsonMap, \n" +
        "col2 \n" +
        "FROM cp.`jsoninput/allTypes.csvh`\n" +
        ")";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("bi", "fl", "st", "mp", "ar", "nu")
        .baselineValues("VARCHAR", "VARCHAR", "VARCHAR", "MAP", "VARCHAR", "NULL")
        .go();

    resetSessionOption(ExecConstants.JSON_ALL_TEXT_MODE);
    resetSessionOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
  }


  @Test
  public void testReadNumbersAsDouble() throws Exception {
    alterSession(ExecConstants.JSON_ALL_TEXT_MODE, false);
    alterSession(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE, true);

    String sql = "SELECT \n" +
        "typeof(jsonMap['bi']) AS bi, \n" +
        "typeof(jsonMap['fl']) AS fl, \n" +
        "typeof(jsonMap['st']) AS st, \n" +
        "typeof(jsonMap['mp']) AS mp, \n" +
        "typeof(jsonMap['ar']) AS ar, \n" +
        "typeof(jsonMap['nu']) AS nu\n" +
        "FROM(\n" +
        "SELECT convert_fromJSON(col1) AS jsonMap, \n" +
        "col2 \n" +
        "FROM cp.`jsoninput/allTypes.csvh`\n" +
        ")";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("bi", "fl", "st", "mp", "ar", "nu")
        .baselineValues("FLOAT8", "FLOAT8", "VARCHAR", "MAP", "FLOAT8", "NULL")
        .go();

    alterSession(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE, true);
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("bi", "fl", "st", "mp", "ar", "nu")
        .baselineValues("FLOAT8", "FLOAT8", "VARCHAR", "MAP", "FLOAT8", "NULL")
        .go();

    resetSessionOption(ExecConstants.JSON_ALL_TEXT_MODE);
    resetSessionOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
  }

  @Test
  public void testReadNumbersAsDoubleFromArgs() throws Exception {
    // Set the system options to make sure that the UDF is using the provided options rather than
    // the system options.
    alterSession(ExecConstants.JSON_ALL_TEXT_MODE, true);
    alterSession(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE, true);
    String sql = "SELECT \n" +
        "typeof(jsonMap['bi']) AS bi, \n" +
        "typeof(jsonMap['fl']) AS fl, \n" +
        "typeof(jsonMap['st']) AS st, \n" +
        "typeof(jsonMap['mp']) AS mp, \n" +
        "typeof(jsonMap['ar']) AS ar, \n" +
        "typeof(jsonMap['nu']) AS nu\n" +
        "FROM(\n" +
        "SELECT convert_fromJSON(col1, false, true) AS jsonMap, \n" +
        "col2 \n" +
        "FROM cp.`jsoninput/allTypes.csvh`\n" +
        ")";

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("bi", "fl", "st", "mp", "ar", "nu")
        .baselineValues("FLOAT8", "FLOAT8", "VARCHAR", "MAP", "FLOAT8", "NULL")
        .go();

    resetSessionOption(ExecConstants.JSON_ALL_TEXT_MODE);
    resetSessionOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE);
  }
}
