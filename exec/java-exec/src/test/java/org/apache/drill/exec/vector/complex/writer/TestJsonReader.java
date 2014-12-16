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
package org.apache.drill.exec.vector.complex.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.RepeatedBigIntVector;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.rules.TemporaryFolder;

public class TestJsonReader extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJsonReader.class);

  private static final boolean VERBOSE_DEBUG = false;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void schemaChange() throws Exception {
    test("select b from dfs.`${WORKING_PATH}/src/test/resources/vector/complex/writer/schemaChange/`");
  }

  @Test
  @Ignore("DRILL-1824")
  public void schemaChangeValidate() throws Exception {
    testBuilder() //
      .sqlQuery("select b from dfs.`${WORKING_PATH}/src/test/resources/vector/complex/writer/schemaChange/`") //
      .unOrdered() //
      .jsonBaselineFile("/vector/complex/writer/expected.json") //
      .build()
      .run();
  }

  public void runTestsOnFile(String filename, UserBitShared.QueryType queryType, String[] queries, long[] rowCounts) throws Exception {
    if (VERBOSE_DEBUG) {
      System.out.println("===================");
      System.out.println("source data in json");
      System.out.println("===================");
      System.out.println(Files.toString(FileUtils.getResourceAsFile(filename), Charsets.UTF_8));
    }

    int i = 0;
    for (String query : queries) {
      if (VERBOSE_DEBUG) {
        System.out.println("=====");
        System.out.println("query");
        System.out.println("=====");
        System.out.println(query);
        System.out.println("======");
        System.out.println("result");
        System.out.println("======");
      }
      int rowCount = testRunAndPrint(queryType, query);
      assertEquals(rowCounts[i], rowCount);
      System.out.println();
      i++;
    }
  }

  @Test
  public void testReadCompressed() throws Exception {
    String filepath = "compressed_json.json";
    File f = folder.newFile(filepath);
    PrintWriter out = new PrintWriter(f);
    out.println("{\"a\" :5}");
    out.close();

    gzipIt(f);
    testBuilder()
        .sqlQuery("select * from dfs.`" + f.getPath() + ".gz" + "`")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(5l)
        .build().run();

    // test reading the uncompressed version as well
    testBuilder()
        .sqlQuery("select * from dfs.`" + f.getPath() + "`")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(5l)
        .build().run();
  }

  public void gzipIt(File sourceFile) throws IOException {

    // modified from: http://www.mkyong.com/java/how-to-compress-a-file-in-gzip-format/
    byte[] buffer = new byte[1024];
    GZIPOutputStream gzos =
        new GZIPOutputStream(new FileOutputStream(sourceFile.getPath() + ".gz"));

    FileInputStream in =
        new FileInputStream(sourceFile);

    int len;
    while ((len = in.read(buffer)) > 0) {
      gzos.write(buffer, 0, len);
    }
    in.close();
    gzos.finish();
    gzos.close();
  }

  @Test
  public void testDrill_1419() throws Exception {
    String[] queries = {"select t.trans_id, t.trans_info.prod_id[0],t.trans_info.prod_id[1] from cp.`/store/json/clicks.json` t limit 5"};
    long[] rowCounts = {5};
    String filename = "/store/json/clicks.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  public void testRepeatedCount() throws Exception {
    test("select repeated_count(str_list) from cp.`/store/json/json_basic_repeated_varchar.json`");
    test("select repeated_count(INT_col) from cp.`/parquet/alltypes_repeated.json`");
    test("select repeated_count(FLOAT4_col) from cp.`/parquet/alltypes_repeated.json`");
    test("select repeated_count(VARCHAR_col) from cp.`/parquet/alltypes_repeated.json`");
    test("select repeated_count(BIT_col) from cp.`/parquet/alltypes_repeated.json`");
  }

  @Test
  public void testRepeatedContains() throws Exception {
    test("select repeated_contains(str_list, 'asdf') from cp.`/store/json/json_basic_repeated_varchar.json`");
    test("select repeated_contains(INT_col, -2147483648) from cp.`/parquet/alltypes_repeated.json`");
    test("select repeated_contains(FLOAT4_col, -1000000000000.0) from cp.`/parquet/alltypes_repeated.json`");
    test("select repeated_contains(VARCHAR_col, 'qwerty' ) from cp.`/parquet/alltypes_repeated.json`");
    test("select repeated_contains(BIT_col, true) from cp.`/parquet/alltypes_repeated.json`");
    test("select repeated_contains(BIT_col, false) from cp.`/parquet/alltypes_repeated.json`");
  }

  @Test
  public void testSingleColumnRead_vector_fill_bug() throws Exception {
    String[] queries = {"select * from cp.`/store/json/single_column_long_file.json`"};
    long[] rowCounts = {13512};
    String filename = "/store/json/single_column_long_file.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  public void testNonExistentColumnReadAlone() throws Exception {
    String[] queries = {"select non_existent_column from cp.`/store/json/single_column_long_file.json`"};
    long[] rowCounts = {13512};
    String filename = "/store/json/single_column_long_file.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  public void testAllTextMode() throws Exception {
    test("alter system set `store.json.all_text_mode` = true");
    String[] queries = {"select * from cp.`/store/json/schema_change_int_to_string.json`"};
    long[] rowCounts = {3};
    String filename = "/store/json/schema_change_int_to_string.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
    test("alter system set `store.json.all_text_mode` = false");
  }

  @Test
  public void readComplexWithStar() throws Exception {
    List<QueryResultBatch> results = testSqlWithResults("select * from cp.`/store/json/test_complex_read_with_star.json`");
    assertEquals(2, results.size());

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
    QueryResultBatch batch = results.get(0);

    assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));
    assertEquals(3, batchLoader.getSchema().getFieldCount());
    testExistentColumns(batchLoader, batch);

    batch.release();
    batchLoader.clear();
  }

  @Test
  public void testNullWhereListExpected() throws Exception {
    test("alter system set `store.json.all_text_mode` = true");
    String[] queries = {"select * from cp.`/store/json/null_where_list_expected.json`"};
    long[] rowCounts = {3};
    String filename = "/store/json/null_where_list_expected.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
    test("alter system set `store.json.all_text_mode` = false");
  }

  @Test
  public void testNullWhereMapExpected() throws Exception {
    test("alter system set `store.json.all_text_mode` = true");
    String[] queries = {"select * from cp.`/store/json/null_where_map_expected.json`"};
    long[] rowCounts = {3};
    String filename = "/store/json/null_where_map_expected.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
    test("alter system set `store.json.all_text_mode` = false");
  }

  @Test
  public void ensureProjectionPushdown() throws Exception {
    // Tests to make sure that we are correctly eliminating schema changing columns.  If completes, means that the projection pushdown was successful.
    test("alter system set `store.json.all_text_mode` = false; "
        + "select  t.field_1, t.field_3.inner_1, t.field_3.inner_2, t.field_4.inner_1 "
        + "from cp.`store/json/schema_change_int_to_string.json` t");
  }

  // The project pushdown rule is correctly adding the projected columns to the scan, however it is not removing
  // the redundant project operator after the scan, this tests runs a physical plan generated from one of the tests to
  // ensure that the project is filtering out the correct data in the scan alone
  @Test
  public void testProjectPushdown() throws Exception {
    String[] queries = {Files.toString(FileUtils.getResourceAsFile("/store/json/project_pushdown_json_physical_plan.json"), Charsets.UTF_8)};
    long[] rowCounts = {3};
    String filename = "/store/json/schema_change_int_to_string.json";
    test("alter system set `store.json.all_text_mode` = false");
    runTestsOnFile(filename, UserBitShared.QueryType.PHYSICAL, queries, rowCounts);

    List<QueryResultBatch> results = testPhysicalWithResults(queries[0]);
    assertEquals(2, results.size());
    // "`field_1`", "`field_3`.`inner_1`", "`field_3`.`inner_2`", "`field_4`.`inner_1`"

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
    QueryResultBatch batch = results.get(0);
    assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

    // this used to be five.  It is now three.  This is because the plan doesn't have a project.
    // Scanners are not responsible for projecting non-existent columns (as long as they project one column)
    assertEquals(3, batchLoader.getSchema().getFieldCount());
    testExistentColumns(batchLoader, batch);

    batch.release();
    batchLoader.clear();
  }

  private void testExistentColumns(RecordBatchLoader batchLoader, QueryResultBatch batch) throws SchemaChangeException {
    VectorWrapper<?> vw = batchLoader.getValueAccessorById(
        RepeatedBigIntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_1")).getFieldIds() //
    );
    assertEquals("[1]", vw.getValueVector().getAccessor().getObject(0).toString());
    assertEquals("[5]", vw.getValueVector().getAccessor().getObject(1).toString());
    assertEquals("[5,10,15]", vw.getValueVector().getAccessor().getObject(2).toString());

    vw = batchLoader.getValueAccessorById(
        IntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_3", "inner_1")).getFieldIds() //
    );
    assertNull(vw.getValueVector().getAccessor().getObject(0));
    assertEquals(2l, vw.getValueVector().getAccessor().getObject(1));
    assertEquals(5l, vw.getValueVector().getAccessor().getObject(2));

    vw = batchLoader.getValueAccessorById(
        IntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_3", "inner_2")).getFieldIds() //
    );
    assertNull(vw.getValueVector().getAccessor().getObject(0));
    assertNull(vw.getValueVector().getAccessor().getObject(1));
    assertEquals(3l, vw.getValueVector().getAccessor().getObject(2));

    vw = batchLoader.getValueAccessorById(
        RepeatedBigIntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_4", "inner_1")).getFieldIds() //
    );
    assertEquals("[]", vw.getValueVector().getAccessor().getObject(0).toString());
    assertEquals("[1,2,3]", vw.getValueVector().getAccessor().getObject(1).toString());
    assertEquals("[4,5,6]", vw.getValueVector().getAccessor().getObject(2).toString());
  }


}
