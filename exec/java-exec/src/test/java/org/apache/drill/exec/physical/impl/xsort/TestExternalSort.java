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
package org.apache.drill.exec.physical.impl.xsort;

import java.io.File;
import java.nio.file.Paths;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.test.TestBuilder;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.file.JsonFileBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SlowTest.class, OperatorTest.class})
public class TestExternalSort extends BaseTestQuery {

  @Test
  public void testNumericTypesManaged() throws Exception {
    testNumericTypes(false);
  }

  @Test
  public void testNumericTypesLegacy() throws Exception {
    testNumericTypes(true);
  }

  /**
   * Test union type support in sort using numeric types: BIGINT and FLOAT8
   * Drill does not support union types fully. Sort was adapted to handle them.
   * This test simply verifies that the sort handles these types, even though
   * Drill does not.
   *
   * @param testLegacy
   *          true to test the old (pre-1.11) sort, false to test the new (1.11
   *          and later) sort
   * @throws Exception
   */

  private void testNumericTypes(boolean testLegacy) throws Exception {
    final int record_count = 10000;
    final String tableDirName = "numericTypes";

    {
      final BatchSchema schema = new SchemaBuilder()
        .add("a", Types.required(TypeProtos.MinorType.INT))
        .build();
      final RowSetBuilder rowSetBuilder = new RowSetBuilder(allocator, schema);

      for (int i = 0; i <= record_count; i += 2) {
        rowSetBuilder.addRow(i);
      }

      final RowSet rowSet = rowSetBuilder.build();
      final File tableFile = createTableFile(tableDirName, "a.json");
      new JsonFileBuilder(rowSet).build(tableFile);
      rowSet.clear();
    }

    {
      final BatchSchema schema = new SchemaBuilder()
        .add("a", Types.required(TypeProtos.MinorType.FLOAT4))
        .build();
      final RowSetBuilder rowSetBuilder = new RowSetBuilder(allocator, schema);

      for (int i = 1; i <= record_count; i += 2) {
        rowSetBuilder.addRow((float) i);
      }

      final RowSet rowSet = rowSetBuilder.build();
      final File tableFile = createTableFile(tableDirName, "b.json");
      new JsonFileBuilder(rowSet)
        .setCustomFormatter("a", "%.2f")
        .build(tableFile);
      rowSet.clear();
    }

    TestBuilder builder = testBuilder()
      .sqlQuery("select * from dfs.`%s` order by a desc", tableDirName)
      .optionSettingQueriesForTestQuery(getOptions(testLegacy))
      .ordered()
      .baselineColumns("a");
    for (int i = record_count; i >= 0; ) {
      builder.baselineValues((long) i--);
      if (i >= 0) {
        builder.baselineValues((double) i--);
      }
    }
    builder.go();
  }

  private String getOptions(boolean testLegacy) {
    String options = "alter session set `exec.enable_union_type` = true";
    options += ";alter session set `"
        + ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION.getOptionName()
        + "` = " + Boolean.toString(testLegacy);
    return options;
  }

  @Test
  @Ignore("Schema changes are disabled in external sort")
  public void testNumericAndStringTypesManaged() throws Exception {
    testNumericAndStringTypes(false);
  }

  @Test
  @Ignore("Schema changes are disabled in external sort")
  public void testNumericAndStringTypesLegacy() throws Exception {
    testNumericAndStringTypes(true);
  }

  private void testNumericAndStringTypes(boolean testLegacy) throws Exception {
    final int record_count = 10000;
    final String tableDirName = "numericAndStringTypes";

    {
      final BatchSchema schema = new SchemaBuilder()
        .add("a", Types.required(TypeProtos.MinorType.INT))
        .build();
      final RowSetBuilder rowSetBuilder = new RowSetBuilder(allocator, schema);

      for (int i = 0; i <= record_count; i += 2) {
        rowSetBuilder.addRow(i);
      }

      final RowSet rowSet = rowSetBuilder.build();
      final File tableFile = createTableFile(tableDirName, "a.json");
      new JsonFileBuilder(rowSet).build(tableFile);
      rowSet.clear();
    }

    {
      final BatchSchema schema = new SchemaBuilder()
        .add("a", Types.required(TypeProtos.MinorType.INT))
        .build();
      final RowSetBuilder rowSetBuilder = new RowSetBuilder(allocator, schema);

      for (int i = 1; i <= record_count; i += 2) {
        rowSetBuilder.addRow(i);
      }

      final RowSet rowSet = rowSetBuilder.build();
      final File tableFile = createTableFile(tableDirName, "b.json");
      new JsonFileBuilder(rowSet)
        .setCustomFormatter("a", "\"%05d\"")
        .build(tableFile);
      rowSet.clear();
    }

    TestBuilder builder = testBuilder()
        .sqlQuery("select * from dfs.`%s` order by a desc", tableDirName)
        .ordered()
        .optionSettingQueriesForTestQuery(getOptions(testLegacy))
        .baselineColumns("a");
    // Strings come first because order by is desc
    for (int i = record_count; i >= 0;) {
      i--;
      if (i >= 0) {
        builder.baselineValues(String.format("%05d", i--));
      }
    }
    for (int i = record_count; i >= 0;) {
      builder.baselineValues((long) i--);
      i--;
    }
    builder.go();
  }

  @Test
  public void testNewColumnsManaged() throws Exception {
    testNewColumns(false);
  }

  @Test
  public void testNewColumnsLegacy() throws Exception {
    testNewColumns(true);
  }

  private void testNewColumns(boolean testLegacy) throws Exception {
    final int record_count = 10000;
    final String tableDirName = "newColumns";

    {
      final BatchSchema schema = new SchemaBuilder()
        .add("a", TypeProtos.MinorType.INT)
        .add("b", TypeProtos.MinorType.INT)
        .build();
      final RowSetBuilder rowSetBuilder = new RowSetBuilder(allocator, schema);

      for (int i = 0; i <= record_count; i += 2) {
        rowSetBuilder.addRow(i, i);
      }

      final RowSet rowSet = rowSetBuilder.build();
      final File tableFile = createTableFile(tableDirName, "a.json");
      new JsonFileBuilder(rowSet).build(tableFile);
      rowSet.clear();
    }

    {
      final BatchSchema schema = new SchemaBuilder()
        .add("a", TypeProtos.MinorType.INT)
        .add("c", TypeProtos.MinorType.INT)
        .build();
      final RowSetBuilder rowSetBuilder = new RowSetBuilder(allocator, schema);

      for (int i = 1; i <= record_count; i += 2) {
        rowSetBuilder.addRow(i, i);
      }

      final RowSet rowSet = rowSetBuilder.build();
      final File tableFile = createTableFile(tableDirName, "b.json");
      new JsonFileBuilder(rowSet).build(tableFile);
      rowSet.clear();
    }

    // Test framework currently doesn't handle changing schema (i.e. new
    // columns) on the client side
    TestBuilder builder = testBuilder()
        .sqlQuery("select a, b, c from dfs.`%s` order by a desc", tableDirName)
        .ordered()
        .optionSettingQueriesForTestQuery(getOptions(testLegacy))
        .baselineColumns("a", "b", "c");
    for (int i = record_count; i >= 0;) {
      builder.baselineValues((long) i, (long) i--, null);
      if (i >= 0) {
        builder.baselineValues((long) i, null, (long) i--);
      }
    }
    builder.go();

    // TODO: Useless test: just dumps to console

    test("select * from dfs.`%s` order by a desc", tableDirName);
  }

  private File createTableFile(final String tableDirName, final String fileName) {
    return dirTestWatcher
      .getRootDir()
      .toPath()
      .resolve(Paths.get(tableDirName, fileName))
      .toFile();
  }
}
