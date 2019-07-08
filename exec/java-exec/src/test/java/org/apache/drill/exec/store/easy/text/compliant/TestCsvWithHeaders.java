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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Sanity test of CSV files with headers.
 * <p>
 * Open issues:
 *
 * <ul>
 * <li>DRILL-7080: A query like SELECT *, dir0 produces the result schema
 * of (dir0, a, b, ...) in V2 and (a, b, ... dir0, dir00) in V3. This
 * seems to be a bug in the Project operator.</li>
 * </ul>
 *
 * The tests assume that the "early schema" mechanism is disabled: that
 * the first batch either contains data, or that the first batch is empty
 * only if there is no data at all to be read.
 *
 * @see {@link TestHeaderBuilder}
 */

@Category(RowSetTests.class)
public class TestCsvWithHeaders extends BaseCsvTest {

  private static final String TEST_FILE_NAME = "basic.csv";

  private static String invalidHeaders[] = {
      "$,,9b,c,c,c_2",
      "10,foo,bar,fourth,fifth,sixth"
  };

  private static String emptyHeaders[] = {
      "",
      "10,foo,bar"
  };

  private static String raggedRows[] = {
      "a,b,c",
      "10,dino",
      "20,foo,bar",
      "30"
  };

  public static final String COLUMNS_FILE_NAME = "columns.csv";

  private static String columnsCol[] = {
      "author,columns",
      "fred,\"Rocks Today,Dino Wrangling\"",
      "barney,Bowlarama"
  };

  @BeforeClass
  public static void setup() throws Exception {
    BaseCsvTest.setup(false,  true);
    buildFile(TEST_FILE_NAME, validHeaders);
    buildNestedTable();
    buildFile(COLUMNS_FILE_NAME, columnsCol);
  }

  /**
   * An empty file with schema is invalid: there is no header line
   * and so there is no schema. It is probably not helpful to return a
   * batch with an empty schema; doing so would simply conflict with the
   * schema of a non-empty file. Also, there is no reason to throw an
   * error; this is not a problem serious enough to fail the query. Instead,
   * we elect to simply return no results at all: no schema and no data.
   * <p>
   * Prior research revealed that most DB engines can handle a null
   * empty result set: no schema, no rows. For example:
   * <br><tt>SELECT * FROM VALUES ();</tt><br>
   * The implementation tested here follows that pattern.
   *
   * @see {@link TestCsvWithoutHeaders#testEmptyFile()}
   */
  @Test
  public void testEmptyFile() throws IOException {
    buildFile(EMPTY_FILE, new String[] {});
    RowSet rowSet = client.queryBuilder().sql(makeStatement(EMPTY_FILE)).rowSet();
    assertNull(rowSet);
  }

  private static final String EMPTY_HEADERS_FILE = "noheaders.csv";

  /**
   * Trivial case: empty header. This case should fail.
   */

  @Test
  public void testEmptyCsvHeaders() throws IOException {
    buildFile(EMPTY_HEADERS_FILE, emptyHeaders);
    try {
      client.queryBuilder().sql(makeStatement(EMPTY_HEADERS_FILE)).run();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("must define at least one header"));
    }
  }

  @Test
  public void testValidCsvHeaders() throws IOException {
    RowSet actual = client.queryBuilder().sql(makeStatement(TEST_FILE_NAME)).rowSet();

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
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

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

  /**
   * Verify that the wildcard expands columns to the header names, including
   * case
   */
  @Test
  public void testWildcard() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

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
   * of just one implicit column. V3 uses non-nullable VARCHAR for file
   * metadata columns.
   */

  @Test
  public void testImplicitColsExplicitSelect() throws IOException {
    String sql = "SELECT A, filename FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("A", MinorType.VARCHAR)
        .add("filename", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", TEST_FILE_NAME)
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * Verify that implicit columns are recognized and populated. Sanity test
   * of just one implicit column. V3 uses non-nullable VARCHAR for file
   * metadata columns.
   */

  @Test
  public void testImplicitColWildcard() throws IOException {
    String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("filename", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar", TEST_FILE_NAME)
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testColsWithWildcard() throws IOException {
    String sql = "SELECT *, a as d FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .add("d", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "bar", "10")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * V3 allows the use of partition columns, even for a non-partitioned file.
   * The columns are null of type Nullable VARCHAR. This is area of Drill
   * is a bit murky: it seems reasonable to support partition columns consistently
   * rather than conditionally based on the structure of the input.
   */
  @Test
  public void testPartitionColsExplicit() throws IOException {
    String sql = "SELECT a, dir0, dir5 FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .addNullable("dir5", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", null, null)
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testDupColumn() throws IOException {
    String sql = "SELECT a, b, a FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("a0", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "foo", "10")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * Test that ragged rows result in the "missing" columns being filled
   * in with the moral equivalent of a null column for CSV: a blank string.
   */
  @Test
  public void testRaggedRows() throws IOException {
    String fileName = "case4.csv";
    buildFile(fileName, raggedRows);
    RowSet actual = client.queryBuilder().sql(makeStatement(fileName)).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "dino", "")
        .addRow("20", "foo", "bar")
        .addRow("30", "", "")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * Test partition expansion .
   * <p>
   * This test is tricky because it will return two data batches
   * (preceded by an empty schema batch.) File read order is random
   * so we have to expect the files in either order.
   * <p>
   * V3, as in V2 before Drill 1.12, puts partition columns after
   * data columns (so that data columns don't shift positions if
   * files are nested to another level.)
   */
  @Test
  public void testPartitionExpansion() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";
    Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();

    RowSet rowSet;
    if (SCHEMA_BATCH_ENABLED) {
      // First batch is empty; just carries the schema.

      assertTrue(iter.hasNext());
      rowSet = iter.next();
      assertEquals(0, rowSet.rowCount());
      rowSet.clear();
    }

    // Read the other two batches.

    for (int i = 0; i < 2; i++) {
      assertTrue(iter.hasNext());
      rowSet = iter.next();

      // Figure out which record this is and test accordingly.

      RowSetReader reader = rowSet.reader();
      assertTrue(reader.next());
      String col1 = reader.scalar(0).getString();
      if (col1.equals("10")) {
        RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
            .addRow("10", "foo", "bar", null)
            .build();
        RowSetUtilities.verify(expected, rowSet);
      } else {
        RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
            .addRow("20", "fred", "wilma", NESTED_DIR)
            .build();
        RowSetUtilities.verify(expected, rowSet);
      }
    }
    assertFalse(iter.hasNext());
  }

  /**
   * Test the use of partition columns with the wildcard. This works for file
   * metadata columns, but confuses the project operator when used for
   * partition columns. DRILL-7080. Still broken in V3 because this appears
   * to be a Project operator issue, not reader issue. Not that the
   * partition column moves after data columns.
   */
  @Test
  public void testWilcardAndPartitionsMultiFiles() throws IOException {
    String sql = "SELECT *, dir0, dir1 FROM `dfs.data`.`%s`";
    Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .addNullable("dir1", MinorType.VARCHAR)
        .addNullable("dir00", MinorType.VARCHAR)
        .addNullable("dir10", MinorType.VARCHAR)
        .buildSchema();

    RowSet rowSet;
    if (SCHEMA_BATCH_ENABLED) {
      // First batch is empty; just carries the schema.

      assertTrue(iter.hasNext());
      rowSet = iter.next();
      RowSetUtilities.verify(new RowSetBuilder(client.allocator(), expectedSchema).build(),
          rowSet);
    }

    // Read the two batches.

    for (int i = 0; i < 2; i++) {
      assertTrue(iter.hasNext());
      rowSet = iter.next();

      // Figure out which record this is and test accordingly.

      RowSetReader reader = rowSet.reader();
      assertTrue(reader.next());
      String aCol = reader.scalar("a").getString();
      if (aCol.equals("10")) {
        RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
            .addRow("10", "foo", "bar", null, null, null, null)
            .build();
        RowSetUtilities.verify(expected, rowSet);
      } else {
        RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
            .addRow("20", "fred", "wilma", NESTED_DIR, null, NESTED_DIR, null)
            .build();
        RowSetUtilities.verify(expected, rowSet);
      }
    }
    assertFalse(iter.hasNext());
  }

   /**
   * Test using partition columns with partitioned files in V3. Although the
   * file is nested to one level, both dir0 and dir1 are nullable VARCHAR.
   * See {@link TestPartitionRace} to show that the types and schemas
   * are consistent even when used across multiple scans.
   */
  @Test
  public void doTestExplicitPartitionsMultiFiles() throws IOException {
    String sql = "SELECT a, b, c, dir0, dir1 FROM `dfs.data`.`%s`";
    Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .addNullable("dir1", MinorType.VARCHAR)
        .buildSchema();

    RowSet rowSet;
    if (SCHEMA_BATCH_ENABLED) {
      // First batch is empty; just carries the schema.

      assertTrue(iter.hasNext());
      rowSet = iter.next();
      RowSetUtilities.verify(new RowSetBuilder(client.allocator(), expectedSchema).build(),
          rowSet);
    }

    // Read the two batches.

    for (int i = 0; i < 2; i++) {
      assertTrue(iter.hasNext());
      rowSet = iter.next();

      // Figure out which record this is and test accordingly.

      RowSetReader reader = rowSet.reader();
      assertTrue(reader.next());
      String aCol = reader.scalar("a").getString();
      if (aCol.equals("10")) {
        RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
            .addRow("10", "foo", "bar", null, null)
            .build();
        RowSetUtilities.verify(expected, rowSet);
      } else {
        RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
            .addRow("20", "fred", "wilma", NESTED_DIR, null)
            .build();
        RowSetUtilities.verify(expected, rowSet);
      }
    }
    assertFalse(iter.hasNext());
  }

  /**
   * The column name `columns` is treated as a plain old
   * column when using column headers.
   */
  @Test
  public void testColumnsCol() throws IOException {
    String sql = "SELECT author, columns FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, COLUMNS_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("author", MinorType.VARCHAR)
        .add("columns", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("fred", "Rocks Today,Dino Wrangling")
        .addRow("barney", "Bowlarama")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * The column name `columns` is treated as a plain old
   * column when using column headers. If used with an index,
   * validation will fail because the VarChar column is not an array
   * @throws Exception
   */
  @Test
  public void testColumnsIndex() throws Exception {
    try {
      String sql = "SELECT author, columns[0] FROM `dfs.data`.`%s`";
      client.queryBuilder().sql(sql, COLUMNS_FILE_NAME).run();
    } catch (UserRemoteException e) {
      assertTrue(e.getMessage().contains(
          "VALIDATION ERROR: Unexpected `columns`[x]; columns array not enabled"));
      assertTrue(e.getMessage().contains("Format plugin: text"));
      assertTrue(e.getMessage().contains("Plugin config name: csv"));
      assertTrue(e.getMessage().contains("Extract headers: true"));
      assertTrue(e.getMessage().contains("Skip first line: false"));
    }
  }

  @Test
  public void testColumnsMissing() throws IOException {
    String sql = "SELECT a, columns FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("columns", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * If columns[x] is used, then this can't possibly match a valid
   * text reader column, so raise an error instead.
   * @throws Exception
   */
  @Test
  public void testColumnsIndexMissing() throws Exception {
    try {
      String sql = "SELECT a, columns[0] FROM `dfs.data`.`%s`";
      client.queryBuilder().sql(sql, TEST_FILE_NAME).run();
    } catch (UserRemoteException e) {
      // Note: this error is caught before reading any tables,
      // so no table information is available.
      assertTrue(e.getMessage().contains(
          "VALIDATION ERROR: Unexpected `columns`[x]; columns array not enabled"));
      assertTrue(e.getMessage().contains("Format plugin: text"));
      assertTrue(e.getMessage().contains("Plugin config name: csv"));
      assertTrue(e.getMessage().contains("Extract headers: true"));
      assertTrue(e.getMessage().contains("Skip first line: false"));
    }
  }

  @Test
  public void testHugeColumn() throws IOException {
    String fileName = buildBigColFile(true);
    String sql = "SELECT * FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, fileName).rowSet();
    assertEquals(10, actual.rowCount());
    RowSetReader reader = actual.reader();
    while (reader.next()) {
      int i = reader.logicalIndex();
      assertEquals(Integer.toString(i + 1), reader.scalar(0).getString());
      String big = reader.scalar(1).getString();
      assertEquals(BIG_COL_SIZE, big.length());
      for (int j = 0; j < BIG_COL_SIZE; j++) {
        assertEquals((char) ((j + i) % 26 + 'A'), big.charAt(j));
      }
      assertEquals(Integer.toString((i + 1) * 10), reader.scalar(2).getString());
    }
    actual.clear();
  }
}
