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

import java.nio.file.Paths;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.complex.writer.TestJsonReader;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests of Drill selected Drill functions using JSON as an input source.
 * (Split from the original <tt>TestJsonReader</tt>.) Relative to the Drill 1.12
 * version, the tests here:
 * <ul>
 * <li>Are rewritten to use the {@link ClusterFixture} framework.</li>
 * <li>Add data verification where missing.</li>
 * <li>Clean up handling of session options.</li>
 * </ul>
 * When running tests, consider these to be secondary. First verify the core
 * JSON reader itself (using {@link TestJsonReader}), then run these tests to
 * ensure vectors populated by JSON work with downstream functions.
 */

@Category(RowSetTests.class)
public class TestJsonReaderFns extends BaseTestJsonReader {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("store", "json"));
    dirTestWatcher.copyResourceToRoot(Paths.get("vector","complex", "writer"));
  }

  @Test
  public void testEmptyList() throws Exception {
    runBoth(() -> doTestEmptyList());
  }

  private void doTestEmptyList() throws Exception {
    final String sql = "select count(a[0]) as ct from dfs.`store/json/emptyLists`";

    final RowSet results = runTest(sql);
    final TupleMetadata schema = new SchemaBuilder()
        .add("ct", MinorType.BIGINT)
        .build();

    final RowSet expected = client.rowSetBuilder(schema)
        .addRow(6L)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  // Expansion of former testRepeatedCount()

  @Test
  public void testRepeatedCountStr() throws Exception {
    runBoth(() -> doTestRepeatedCountStr());
  }

  private void doTestRepeatedCountStr() throws Exception {
    final RowSet results = runTest("select repeated_count(str_list) from cp.`store/json/json_basic_repeated_varchar.json`");
    final RowSet expected = client.rowSetBuilder(countSchema())
        .addSingleCol(5)
        .addSingleCol(1)
        .addSingleCol(3)
        .addSingleCol(1)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedCountInt() throws Exception {
    runBoth(() -> doTestRepeatedCountInt());
  }

  private void doTestRepeatedCountInt() throws Exception {
    final RowSet results = runTest("select repeated_count(INT_col) from cp.`parquet/alltypes_repeated.json`");
    final RowSet expected = client.rowSetBuilder(countSchema())
        .addSingleCol(12)
        .addSingleCol(4)
        .addSingleCol(4)
        .addSingleCol(4)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedCountFloat4() throws Exception {
    runBoth(() -> doTestRepeatedCountFloat4());
  }

  private void doTestRepeatedCountFloat4() throws Exception {
    final RowSet results = runTest("select repeated_count(FLOAT4_col) from cp.`parquet/alltypes_repeated.json`");
    final RowSet expected = client.rowSetBuilder(countSchema())
        .addSingleCol(7)
        .addSingleCol(4)
        .addSingleCol(4)
        .addSingleCol(4)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedCountVarchar() throws Exception {
    runBoth(() -> doTestRepeatedCountVarchar());
  }

  private void doTestRepeatedCountVarchar() throws Exception {
    final RowSet results = runTest("select repeated_count(VARCHAR_col) from cp.`parquet/alltypes_repeated.json`");
    final RowSet expected = client.rowSetBuilder(countSchema())
        .addSingleCol(4)
        .addSingleCol(3)
        .addSingleCol(3)
        .addSingleCol(3)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedCountBit() throws Exception {
    runBoth(() -> doTestRepeatedCountBit());
  }

  private void doTestRepeatedCountBit() throws Exception {
    final RowSet results = runTest("select repeated_count(BIT_col) from cp.`parquet/alltypes_repeated.json`");
    final RowSet expected = client.rowSetBuilder(countSchema())
        .addSingleCol(7)
        .addSingleCol(7)
        .addSingleCol(5)
        .addSingleCol(3)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  private TupleMetadata countSchema() {
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .add("EXPR$0", MinorType.INT)
        .build();
    return expectedSchema;
  }


  // Reimplementation of testRepeatedContains()

  @Test
  public void testRepeatedContainsStr() throws Exception {
    runBoth(() -> doTestRepeatedContainsStr());
  }

  private void doTestRepeatedContainsStr() throws Exception {
    final RowSet results = runTest("select repeated_contains(str_list, 'asdf') from cp.`store/json/json_basic_repeated_varchar.json`");
    final RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(2) // WRONG! Should be 1 (true). See DRILL-6034
        .addSingleCol(0)
        .addSingleCol(1)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedContainsInt() throws Exception {
    runBoth(() -> doTestRepeatedContainsInt());
  }

  private void doTestRepeatedContainsInt() throws Exception {
    final RowSet results = runTest("select repeated_contains(INT_col, -2147483648) from cp.`parquet/alltypes_repeated.json`");
    final RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(1)
        .addSingleCol(0)
        .addSingleCol(0)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedContainsFloat4() throws Exception {
    runBoth(() -> doTestRepeatedContainsFloat4());
  }

  private void doTestRepeatedContainsFloat4() throws Exception {
    final RowSet results = runTest("select repeated_contains(FLOAT4_col, -1000000000000.0) from cp.`parquet/alltypes_repeated.json`");
    final RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(1)
        .addSingleCol(0)
        .addSingleCol(0)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedContainsVarchar() throws Exception {
    runBoth(() -> doTestRepeatedContainsVarchar());
  }

  private void doTestRepeatedContainsVarchar() throws Exception {
    final RowSet results = runTest("select repeated_contains(VARCHAR_col, 'qwerty' ) from cp.`parquet/alltypes_repeated.json`");
    final RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(1)
        .addSingleCol(0)
        .addSingleCol(0)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedContainsBitTrue() throws Exception {
    runBoth(() -> doTestRepeatedContainsBitTrue());
  }

  private void doTestRepeatedContainsBitTrue() throws Exception {
    final RowSet results = runTest("select repeated_contains(BIT_col, true) from cp.`parquet/alltypes_repeated.json`");
    final RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(11) // WRONG! Should be 1 (true). See DRILL-6034
        .addSingleCol(2)
        .addSingleCol(0)
        .addSingleCol(3)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRepeatedContainsBitFalse() throws Exception {
    runBoth(() -> doTestRepeatedContainsBitFalse());
  }

  private void doTestRepeatedContainsBitFalse() throws Exception {
    final RowSet results = runTest("select repeated_contains(BIT_col, false) from cp.`parquet/alltypes_repeated.json`");
    final RowSet expected = client.rowSetBuilder(bitCountSchema())
        .addSingleCol(5) // WRONG! Should be 1 (true). See DRILL-6034
        .addSingleCol(5)
        .addSingleCol(5)
        .addSingleCol(0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  private TupleMetadata bitCountSchema() {
    return new SchemaBuilder()
        .add("EXPR$0", MinorType.BIT)
        .buildSchema();
  }
}
