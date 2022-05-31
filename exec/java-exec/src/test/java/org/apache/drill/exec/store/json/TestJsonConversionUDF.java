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


import ch.qos.logback.classic.Level;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.project.ProjectRecordBatch;
import org.apache.drill.exec.physical.impl.validate.IteratorValidatorBatchIterator;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.LogFixture;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestJsonConversionUDF extends ClusterTest {

  protected static LogFixture logFixture;
  private final static Level CURRENT_LOG_LEVEL = Level.DEBUG;
  @BeforeClass
  public static void setup() throws Exception {
    logFixture = LogFixture.builder()
      .toConsole()
      .logger(ProjectRecordBatch.class, CURRENT_LOG_LEVEL)
      .logger(JsonLoaderImpl.class, CURRENT_LOG_LEVEL)
      .logger(IteratorValidatorBatchIterator.class, CURRENT_LOG_LEVEL)
      .build();

    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testConvertFromJsonFunctionWithBinaryInput() throws Exception {
    client.alterSession(ExecConstants.JSON_READER_NAN_INF_NUMBERS, true);
    String sql = "SELECT string_binary(convert_toJSON(convert_fromJSON(columns[1]))) as col FROM cp.`jsoninput/nan_test.csv`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals("Query result must contain 1 row", 1, results.rowCount());

    results.print();
  }

  @Test
  public void testConvertFromJSONWithStringInput() throws Exception {
    // String sql = "SELECT *, convert_FromJSON('{\"foo\":\"bar\"}') FROM cp.`jsoninput/allTypes.csv`";
    String sql = "SELECT convert_FromJSON('{\"foo\":\"bar\"}') FROM (VALUES(1))";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    results.print();
  }

/*
  private void doTestConvertToJsonFunction() throws Exception {
    String table = "nan_test.csv";
    File file = new File(dirTestWatcher.getRootDir(), table);
    String csv = "col_0, {\"nan_col\":NaN}";
    String query = String.format("select string_binary(convert_toJSON(convert_fromJSON(columns[1]))) as col " +
      "from dfs.`%s` where columns[0]='col_0'", table);
    try {
      FileUtils.writeStringToFile(file, csv,  Charset.defaultCharset());
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
  public void testConvertFromJsonFunction() throws Exception {
    //runBoth(this::doTestConvertFromJsonFunction);
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
  */

}
