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
package org.apache.drill.exec.store.easy.text.compliant;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SQL-level tests for CSV headers. See
 * {@link TestHeaderBuilder} for detailed unit tests.
 * This test does not attempt to duplicate all the cases
 * from the unit tests; instead it just does a sanity check.
 */

public class TestCsv extends ClusterTest {

  private static final String CASE2_FILE_NAME = "case2.csv";

  private static File testDir;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher).maxParallelization(1));

    // Set up CSV storage plugin using headers.

    TextFormatConfig csvFormat = new TextFormatConfig();
    csvFormat.fieldDelimiter = ',';
    csvFormat.skipFirstLine = false;
    csvFormat.extractHeader = true;

    testDir = cluster.makeDataDir("data", "csv", csvFormat);
    buildFile(CASE2_FILE_NAME, validHeaders);
  }

  private static String emptyHeaders[] = {
      "",
      "10,foo,bar"
  };

  @Test
  public void testEmptyCsvHeaders() throws IOException {
    String fileName = "case1.csv";
    buildFile(fileName, emptyHeaders);
    try {
      client.queryBuilder().sql(makeStatement(fileName)).run();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("must define at least one header"));
    }
  }

  private static String validHeaders[] = {
      "a,b,c",
      "10,foo,bar"
  };

  @Test
  public void testValidCsvHeaders() throws IOException {
    RowSet actual = client.queryBuilder().sql(makeStatement(CASE2_FILE_NAME)).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  private static String invalidHeaders[] = {
      "$,,9b,c,c,c_2",
      "10,foo,bar,fourth,fifth,sixth"
  };

  @Test
  public void testInvalidCsvHeaders() throws IOException {
    String fileName = "case3.csv";
    buildFile(fileName, invalidHeaders);
    RowSet actual = client.queryBuilder().sql(makeStatement(fileName)).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("column_1", MinorType.VARCHAR)
        .add("column_2", MinorType.VARCHAR)
        .add("col_9b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("c_2", MinorType.VARCHAR)
        .add("c_2_2", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar", "fourth", "fifth", "sixth")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  // Test fix for DRILL-5590
  @Test
  public void testCsvHeadersCaseInsensitive() throws IOException {
    String sql = "SELECT A, b, C FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, CASE2_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("C", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  private String makeStatement(String fileName) {
    return "SELECT * FROM `dfs.data`.`" + fileName + "`";
  }

  private static void buildFile(String fileName, String[] data) throws IOException {
    try(PrintWriter out = new PrintWriter(new FileWriter(new File(testDir, fileName)))) {
      for (String line : data) {
        out.println(line);
      }
    }
  }

  /**
   * Verify that the wildcard expands columns to the header names, including
   * case
   */
  @Test
  public void testWildcard() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, CASE2_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * Verify that implicit columns are recognized and populated. Sanity test
   * of just one implicit column.
   */
  @Test
  public void testImplicitColsExplicitSelect() throws IOException {
    String sql = "SELECT A, filename FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, CASE2_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .addNullable("filename", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", CASE2_FILE_NAME)
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * Verify that implicit columns are recognized and populated. Sanity test
   * of just one implicit column.
   */
  @Test
  public void testImplicitColsWildcard() throws IOException {
    String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, CASE2_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .addNullable("filename", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar", CASE2_FILE_NAME)
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * CSV does not allow explicit use of dir0, dir1, etc. columns. Treated
   * as undefined nullable int columns.
   * <p>
   * Note that the class path storage plugin does not support directories
   * (partitions). It is unclear if that should show up here as the
   * partition column names being undefined (hence Nullable INT) or should
   * they still be defined, but set to a null Nullable VARCHAR?
   */
  @Test
  public void testPartitionColsWildcard() throws IOException {
    String sql = "SELECT *, dir0, dir5 FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, CASE2_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.INT)
        .addNullable("dir5", MinorType.INT)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar", null, null)
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * CSV does not allow explicit use of dir0, dir1, etc. columns. Treated
   * as undefined nullable int columns.
   */
  @Test
  public void testPartitionColsExplicit() throws IOException {
    String sql = "SELECT a, dir0, dir5 FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, CASE2_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.INT)
        .addNullable("dir5", MinorType.INT)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", null, null)
        .build();
    RowSetUtilities.verify(expected, actual);
  }
}
