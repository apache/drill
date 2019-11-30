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

import static org.apache.drill.test.TestBuilder.listOf;
import static org.apache.drill.test.TestBuilder.mapOf;
import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleMap;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.QueryResultSet;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Reimplementation of selected tests from the
 * TestJsonReader test suite.
 */

@Category(RowSetTests.class)
public class TestJsonReaderQueries extends BaseTestJsonReader {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("store", "json"));
    dirTestWatcher.copyResourceToRoot(Paths.get("vector","complex", "writer"));
    dirTestWatcher.copyResourceToRoot(Paths.get("jsoninput/drill_3353"));
  }

  /**
   * Reimplementation of a Drill 1.12 unit test to actually verify results.
   * Doing so is non-trivial as inline comments explain. This test shows the
   * limits "schema-free" processing when the schema changes.
   * @throws Exception
   */

  @Test
  @Ignore("Too fragile to keep working")
  public void schemaChange() throws Exception {
    String sql = "select b from dfs.`vector/complex/writer/schemaChange/`";
//    runAndPrint(sql);
    QueryResultSet results = client.queryBuilder().sql(sql).resultSet();

    // Query will scan two files:
    // f1:
    // {"a": "foo","b": null}
    // {"a": "bar","b": null}
    // f2:
    // {"a": "foo2","b": null}
    // {"a": "bar2","b": {"x":1, "y":2}}

    // When f1 is read, we didn't know the type of b, so it will default to Varchar
    // (Assuming text mode for that column.)
    //
    // On reading f2, we discover that b is a map (which we discover the
    // second record.)
    //
    // The scanner handles schema persistence, but not (at present) for maps.
    // If we did have schema persistence, then if f2 was first, we'd remember
    // the map schema when we read f1.
    //
    // This crazy behavior is the best we can do without a schema. Bottom line:
    // Drill needs a user-provided schema to make sense of these cases because
    // "Drill can't predict the future" (TM).
    //
    // See TestCSV* for a way to implement this test case

    TupleMetadata f2Schema = new SchemaBuilder()
        .addMap("b")
          .addNullable("x", MinorType.BIGINT)
          .addNullable("y", MinorType.BIGINT)
          .resumeSchema()
        .build();
    RowSet f2Expected = client.rowSetBuilder(f2Schema)
        .addSingleCol(mapValue(null, null))
        .addSingleCol(mapValue(1L, 2L))
        .build();

    TupleMetadata f1Schema = new SchemaBuilder()
        .addNullable("b", MinorType.VARCHAR)
        .build();
    RowSet f1Expected = client.rowSetBuilder(f1Schema)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();

    // First batch is empty; presents only schema. But,
    // since file order is non-deterministic, we don't know
    // which one.

    RowSet batch = results.next();
    assertNotNull(batch);
    assertEquals(0, batch.rowCount());
    boolean mapFirst;
    if (batch.schema().metadata("b").type() == MinorType.MAP) {
      RowSet expected = client.rowSetBuilder(f2Schema)
          .build();
      RowSetUtilities.verify(expected, batch);
      mapFirst = true;
    } else {
      RowSet expected = client.rowSetBuilder(f1Schema)
          .build();
      RowSetUtilities.verify(expected, batch);
      mapFirst = false;
    }
    for (int i = 0; i < 2; i++) {
      batch = results.next();
      assertNotNull(batch);
      if (i == 0 && mapFirst || i == 1 && ! mapFirst) {
        RowSetUtilities.verify(f2Expected, batch);
      } else {
        RowSetUtilities.verify(f1Expected, batch);
      }
    }
    assertNull(results.next());
    results.close();
  }

  /**
   * Reimplementation of the Drill 1.12 test. Tests the odd case in which
   * we project both a single column from inside a map, as well as the
   * entire map.
   *
   *
   * As it turns out, the original functionality
   * was broken, that the test had incorrect expected results that reflected the broken
   * functionality.
   * <p>
   * The query selects two fields which are deeply nested:
   * <ul>
   * <li><tt>t.field_4.inner_3</tt> where <tt>field_4</tt> is a map and
   * <tt>inner_3</tt> is another map.</li>
   * <li><tt>t.field_4</tt> is a map with three total items.</li>
   * </ul>
   * The original expected results
   * @throws Exception
   */

  @Test
  @Ignore("broken")
  public void testFieldSelectionBug() throws Exception {
    runBoth(() -> doTestFieldSelectionBug());
  }

  private void doTestFieldSelectionBug() throws Exception {
    String sql = "select t.field_4.inner_3 as col_1, t.field_4 as col_2 from cp.`store/json/schema_change_int_to_string.json` t";
    try {
      client.alterSession(ExecConstants.JSON_ALL_TEXT_MODE, true);

      testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns("col_1", "col_2")
          .baselineValues(
              mapOf(),
              mapOf(
                  "inner_1", listOf(),
                  "inner_3", mapOf()))
          .baselineValues(
              mapOf("inner_object_field_1", "2"),
              mapOf(
                  "inner_1", listOf("1", "2", "3"),
                  "inner_2", "3",
                  "inner_3", mapOf("inner_object_field_1", "2")))
          .baselineValues(
              mapOf(),
              mapOf(
                  "inner_1", listOf("4", "5", "6"),
                  "inner_2", "3",
                  "inner_3", mapOf()))
          .go();
    } finally {
      client.resetSession(ExecConstants.JSON_ALL_TEXT_MODE);
    }
  }

  @Test
  public void testReadCompressed() throws Exception {
    runBoth(() -> doTestReadCompressed());
  }

  private void doTestReadCompressed() throws Exception {
    String filepath = "compressed_json.json";
    File f = new File(dirTestWatcher.getRootDir(), filepath);
    PrintWriter out = new PrintWriter(f);
    out.println("{\"a\" :5}");
    out.close();

    gzipIt(f);
    testBuilder()
        .sqlQuery("select * from dfs.`%s.gz`", filepath)
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(5l)
        .build().run();

    // test reading the uncompressed version as well
    testBuilder()
        .sqlQuery("select * from dfs.`%s`", filepath)
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(5l)
        .build().run();
  }

  public static void gzipIt(File sourceFile) throws IOException {

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
    runBoth(() -> doTestDrill_1419());
  }

  private void doTestDrill_1419() throws Exception {
    String sql = "select t.trans_id, t.trans_info.prod_id[0],t.trans_info.prod_id[1] from cp.`store/json/clicks.json` t limit 5";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("trans_id", MinorType.BIGINT)
        .addNullable("EXPR$1", MinorType.BIGINT)
        .addNullable("EXPR$2", MinorType.BIGINT)
        .build();

    RowSet expected = client.rowSetBuilder(schema)
        .addRow(31920L, 174L, 2L)
        .addRow(31026L, null, null)
        .addRow(33848L, 582L, null)
        .addRow(32383L, 710L, 47L)
        .addRow(32359L, 0L, 8L)
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testSingleColumnRead_vector_fill_bug() throws Exception {
    runBoth(() -> doTestSingleColumnRead_vector_fill_bug());
  }

  private void doTestSingleColumnRead_vector_fill_bug() throws Exception {
    String sql = "select * from cp.`store/json/single_column_long_file.json`";
    QuerySummary results = client.queryBuilder().sql(sql).run();
    assertEquals(13_512, results.recordCount());
  }

  @Test
  public void testNonExistentColumnReadAlone() throws Exception {
    runBoth(() -> doTestNonExistentColumnReadAlone());
  }

  private void doTestNonExistentColumnReadAlone() throws Exception {
    String sql = "select non_existent_column from cp.`store/json/single_column_long_file.json`";
    QuerySummary results = client.queryBuilder().sql(sql).run();
    assertEquals(13_512, results.recordCount());
  }

  @Test
  public void testAllTextMode() throws Exception {
    runBoth(() -> doTestAllTextMode());
  }

  private void doTestAllTextMode() throws Exception {
    client.alterSession(ExecConstants.JSON_ALL_TEXT_MODE, true);
    try {
      String sql = "select * from cp.`store/json/schema_change_int_to_string.json`";
      QuerySummary results = client.queryBuilder().sql(sql).run();

      // This is a pretty lame test as it does not verify results. However,
      // enough other all-text mode tests do verify results. Here, we just
      // make sure that the query does not die with a schema change exception.

      assertEquals(3, results.recordCount());
    } finally {
      client.resetSession(ExecConstants.JSON_ALL_TEXT_MODE);
    }
  }

  private void testExistentColumns(RowSet result) throws SchemaChangeException {

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("field_1", MinorType.BIGINT)
        .addMap("field_3")
          .addNullable("inner_1", MinorType.BIGINT)
          .addNullable("inner_2", MinorType.BIGINT)
          .resumeSchema()
        .addMap("field_4")
          .addArray("inner_1", MinorType.BIGINT)
          .addNullable("inner_2", MinorType.BIGINT)
          .resumeSchema()
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(longArray(1L), mapValue(null, null), mapValue(longArray(), null))
        .addRow(longArray(5L), mapValue(2L, null), mapValue(longArray(1L, 2L, 3L), 3L))
        .addRow(longArray(5L, 10L, 15L), mapValue(5L, 3L), mapValue(longArray(4L, 5L, 6L), 3L))
        .build();

    RowSetUtilities.verify(expected, result);
  }

  @Test
  public void readComplexWithStar() throws Exception {
    runBoth(() -> doReadComplexWithStar());
  }

  private void doReadComplexWithStar() throws Exception {
    RowSet results = runTest("select * from cp.`store/json/test_complex_read_with_star.json`");
    testExistentColumns(results);
  }

  @Test
  public void testNullWhereListExpectedNumeric() throws Exception {
    runBoth(() -> doTestNullWhereListExpectedNumeric());
  }

  private void doTestNullWhereListExpectedNumeric() throws Exception {
    String sql = "select * from cp.`store/json/null_where_list_expected.json`";
    RowSet results = runTest(sql);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("list_1", MinorType.BIGINT)
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addSingleCol(longArray(1L, 2L, 3L))
        .addSingleCol(longArray())
        .addSingleCol(longArray(4L, 5L, 6L))
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testNullWhereMapExpectedNumeric() throws Exception {
    runBoth(() -> doTestNullWhereMapExpectedNumeric());
  }

  private void doTestNullWhereMapExpectedNumeric() throws Exception {
    String sql = "select * from cp.`store/json/null_where_map_expected.json`";
    RowSet results = runTest(sql);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("map_1")
          .addNullable("f_1", MinorType.BIGINT)
          .addNullable("f_2", MinorType.BIGINT)
          .addNullable("f_3", MinorType.BIGINT)
          .resumeSchema()
        .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addSingleCol(mapValue(1L, 2L, 3L))
        .addSingleCol(mapValue(null, null, null))
        .addSingleCol(mapValue(3L, 4L, 5L))
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testNullWhereMapExpectedText() throws Exception {
    runBoth(() -> doTestNullWhereMapExpectedText());
  }

  private void doTestNullWhereMapExpectedText() throws Exception {
    client.alterSession(ExecConstants.JSON_ALL_TEXT_MODE, true);
    try {
      String sql = "select * from cp.`store/json/null_where_map_expected.json`";
      RowSet results = runTest(sql);

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addMap("map_1")
            .addNullable("f_1", MinorType.VARCHAR)
            .addNullable("f_2", MinorType.VARCHAR)
            .addNullable("f_3", MinorType.VARCHAR)
            .resumeSchema()
          .build();

      RowSet expected = client.rowSetBuilder(expectedSchema)
          .addSingleCol(mapValue("1", "2", "3"))
          .addSingleCol(mapValue(null, null, null))
          .addSingleCol(mapValue("3", "4", "5"))
          .build();

      RowSetUtilities.verify(expected, results);
    } finally {
      client.resetSession(ExecConstants.JSON_ALL_TEXT_MODE);
    }
  }

  @Test
  public void testNullWhereListExpectedText() throws Exception {
    runBoth(() -> doTestNullWhereListExpectedText());
  }

  private void doTestNullWhereListExpectedText() throws Exception {
    client.alterSession(ExecConstants.JSON_ALL_TEXT_MODE, true);
    try {
      String sql = "select * from cp.`store/json/null_where_list_expected.json`";
      RowSet results = runTest(sql);

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addArray("list_1", MinorType.VARCHAR)
          .build();

      RowSet expected = client.rowSetBuilder(expectedSchema)
          .addSingleCol(strArray("1", "2", "3"))
          .addSingleCol(strArray())
          .addSingleCol(strArray("4", "5", "6"))
          .build();

      RowSetUtilities.verify(expected, results);
    } finally {
      client.resetSession(ExecConstants.JSON_ALL_TEXT_MODE);
    }
  }

  @Test
  public void ensureProjectionPushdown() throws Exception {
    runBoth(() -> doEnsureProjectionPushdown());
  }

  private void doEnsureProjectionPushdown() throws Exception {
    // Tests to make sure that we are correctly eliminating schema changing columns.
    // If completes, means that the projection pushdown was successful.

    client.alterSession(ExecConstants.JSON_ALL_TEXT_MODE, false);
    try {
      String sql = "select t.field_1, t.field_3.inner_1, t.field_3.inner_2, t.field_4.inner_1 "
                  + "from cp.`store/json/schema_change_int_to_string.json` t";
      assertEquals(3, client.queryBuilder().sql(sql).run().recordCount());
    } finally {
      client.resetSession(ExecConstants.JSON_ALL_TEXT_MODE);
    }
  }

  /**
   * Old description: The project pushdown rule is correctly adding the
   * projected columns to the scan, however it is not removing the redundant
   * project operator after the scan, this tests runs a physical plan generated
   * from one of the tests to ensure that the project is filtering out the
   * correct data in the scan alone.
   * <p>
   * Revised functionality: the scan operator does all of the requested project
   * operations, producing five columns.
   */

  @Test
  public void testProjectPushdown() throws Exception {
    try {
      enableV2Reader(true);
      client.alterSession(ExecConstants.JSON_ALL_TEXT_MODE, false);
      String plan = Files.asCharSource(DrillFileUtils.getResourceAsFile(
          "/store/json/project_pushdown_json_physical_plan.json"),
          Charsets.UTF_8).read();
//      client.queryBuilder().physical(plan).printCsv();
      DirectRowSet results = client.queryBuilder().physical(plan).rowSet();
//      results.print();

      // Projects all columns (since the revised scan operator handles missing-column
      // projection.) Note that the result includes two batches, including the first empty
      // batch.

      TupleMetadata schema = new SchemaBuilder()
          .addArray("field_1", MinorType.BIGINT)
          .addMap("field_3")
            .addNullable("inner_1", MinorType.BIGINT)
            .addNullable("inner_2", MinorType.BIGINT)
            .resumeSchema()
          .addMap("field_4")
            .addArray("inner_1", MinorType.BIGINT)
            .resumeSchema()
          .addNullable("non_existent_at_root", MinorType.VARCHAR)
          .addMap("non_existent")
            .addMap("nested")
              .addNullable("field", MinorType.VARCHAR)
              .resumeMap()
            .resumeSchema()
          .build();

      Object nullMap = singleMap(singleMap(null));
      RowSet expected = client.rowSetBuilder(schema)
          .addRow(longArray(1L), mapValue(null, null), singleMap(longArray()), null, nullMap )
          .addRow(longArray(5L), mapValue(2L, null), singleMap(longArray(1L, 2L, 3L)), null, nullMap)
          .addRow(longArray(5L, 10L, 15L), mapValue(5L, 3L), singleMap(longArray(4L, 5L, 6L)), null, nullMap)
          .build();
      RowSetUtilities.verify(expected, results);
    } finally {
      client.resetSession(ExecConstants.JSON_ALL_TEXT_MODE);
      resetV2Reader();
    }
  }

  @Test
  public void testJsonDirectoryWithEmptyFile() throws Exception {
    runBoth(() -> doTestJsonDirectoryWithEmptyFile());
  }

  private void doTestJsonDirectoryWithEmptyFile() throws Exception {
    testBuilder()
        .sqlQuery("select * from dfs.`store/json/jsonDirectoryWithEmpyFile`")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(1l)
        .build()
        .run();
  }

  // Only works in V2 reader.
  // Disabled because it depends on the (random) read order

  @Test
  @Ignore("unstable")
  public void drill_4032() throws Exception {
    try {
      enableV2Reader(true);
      File table_dir = dirTestWatcher.makeTestTmpSubDir(Paths.get("drill_4032"));
      table_dir.mkdir();
      try (PrintWriter os = new PrintWriter(new FileWriter(new File(table_dir, "a.json")))) {
        os.write("{\"col1\": \"val1\", \"col2\": null}");
        os.write("{\"col1\": \"val2\", \"col2\": {\"col3\":\"abc\", \"col4\":\"xyz\"}}");
      }
      try (PrintWriter os = new PrintWriter(new FileWriter(new File(table_dir, "b.json")))) {
        os.write("{\"col1\": \"val3\", \"col2\": null}");
        os.write("{\"col1\": \"val4\", \"col2\": null}");
      }
      String sql = "select t.col1, t.col2.col3 from dfs.tmp.drill_4032 t order by col1";
//      String sql = "select t.col1, t.col2.col3 from dfs.tmp.drill_4032 t";
      RowSet results = runTest(sql);
      results.print();

      TupleMetadata schema = new SchemaBuilder()
          .addNullable("col1", MinorType.VARCHAR)
          .addNullable("EXPR$1", MinorType.VARCHAR)
          .build();

      RowSet expected = client.rowSetBuilder(schema)
          .addRow("val1", null)
          .addRow("val2", "abc")
          .addRow("val3", null)
          .addRow("val4", null)
          .build();
      RowSetUtilities.verify(expected, results);
    } finally {
      resetV2Reader();
    }
  }

  /** Test <pre>
   * { "a": 5.2 }
   * { "a": 6 }</pre>
   * In Drill 1.16 and before, triggered an exception. In Drill 1.17
   * and later, the second number, an integer, is converted to a
   * double.
   */

  @Test
  public void testMixedNumberTypes() throws Exception {
    try {
      enableV2Reader(true);
      String sql = "select * from cp.`jsoninput/mixed_number_types.json`";
      RowSet results = runTest(sql);
      TupleMetadata schema = new SchemaBuilder()
          .addNullable("a", MinorType.FLOAT8)
          .build();

      RowSet expected = client.rowSetBuilder(schema)
          .addSingleCol(5.2D)
          .addSingleCol(6.0D)
          .build();
      RowSetUtilities.verify(expected, results);
    } finally {
      resetV2Reader();
    }
  }
}
