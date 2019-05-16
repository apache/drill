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
 * Sanity test of CSV files with headers. Tests both the original
 * "compliant" version and the V3 version based on the row set
 * framework.
 * <p>
 * The CSV reader is a "canary in the coal mine" for many scan features.
 * It turns out that there are several bugs in "V2" (AKA "new text reader")
 * that are fixed in "V3" (the one based on the row set framework), and one
 * that is not yet fixed.
 *
 * <ul>
 * <li>Ragged rows will crash the V2 text reader when headers are used.
 * No V2 test exists as a result. Fixed in V3.</li>
 * <li>DRILL-7083: in V2, if files are nested to 2 levels, but we ask
 * for dir2 (the non-existent third level), the type of dir2 will be
 * nullable INT. In V3, the type is Nullable VARCHAR (just like for the
 * existing partition levels.)</li>
 * <li>DRILL-7080: A query like SELECT *, dir0 produces the result schema
 * of (dir0, a, b, ...) in V2 and (a, b, ... dir0, dir00) in V3. This
 * seems to be a bug in the Project operator.</li>
 * </ul>
 *
 * The V3 tests all demonstrate that the row set scan framework
 * delivers a first empty batch from each scan. I (Paul) had understood
 * that we had an "fast schema" path as the result of the "empty batch"
 * project. However, the V2 reader does not provide the schema-only
 * first batch. So, not sure if doing so is a feature, or a bug because
 * things changed. Easy enough to change if we choose to. If so, the
 * tests here would remove the test for that schema-only batch.
 * <p>
 * Tests are run for both V2 and V3. When the results are the same,
 * the test occurs once, wrapped in a "driver" to select V2 or V3 mode.
 * When behavior differs, there are separate tests for V2 and V3.
 * <p>
 * The V2 tests are temporary. Once we accept that V3 is stable, we
 * can remove V2 (and the "old text reader.") The behavior in V3 is
 * more correct, no reason to keep the old, broken behavior.
 *
 * @see {@link TestHeaderBuilder}
 */

// CSV reader now hosted on the row set framework
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

  private static final String EMPTY_FILE = "empty.csv";

  @Test
  public void testEmptyFile() throws IOException {
    buildFile(EMPTY_FILE, new String[] {});
    try {
      enableV3(false);
      doTestEmptyFile();
      enableV3(true);
      doTestEmptyFile();
    } finally {
      resetV3();
    }
  }

  private void doTestEmptyFile() throws IOException {
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
      enableV3(false);
      doTestEmptyCsvHeaders();
      enableV3(true);
      doTestEmptyCsvHeaders();
    } finally {
      resetV3();
    }
  }

  private void doTestEmptyCsvHeaders() throws IOException {
    try {
      client.queryBuilder().sql(makeStatement(EMPTY_HEADERS_FILE)).run();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("must define at least one header"));
    }
  }

  @Test
  public void testValidCsvHeaders() throws IOException {
    try {
      enableV3(false);
      doTestValidCsvHeaders();
      enableV3(true);
      doTestValidCsvHeaders();
    } finally {
      resetV3();
    }
  }

  private void doTestValidCsvHeaders() throws IOException {
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
    try {
      enableV3(false);
      doTestInvalidCsvHeaders();
      enableV3(true);
      doTestInvalidCsvHeaders();
    } finally {
      resetV3();
    }
  }

  private void doTestInvalidCsvHeaders() throws IOException {
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

  @Test
  public void testCsvHeadersCaseInsensitive() throws IOException {
    try {
      enableV3(false);
      doTestCsvHeadersCaseInsensitive();
      enableV3(true);
      doTestCsvHeadersCaseInsensitive();
    } finally {
      resetV3();
    }
  }

  // Test fix for DRILL-5590
  private void doTestCsvHeadersCaseInsensitive() throws IOException {
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

  @Test
  public void testWildcard() throws IOException {
    try {
      enableV3(false);
      doTestWildcard();
      enableV3(true);
      doTestWildcard();
    } finally {
      resetV3();
    }
  }

  /**
   * Verify that the wildcard expands columns to the header names, including
   * case
   */
  private void doTestWildcard() throws IOException {
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
   * of just one implicit column. V2 uses nullable VARCHAR for file
   * metadata columns.
   */

  @Test
  public void testImplicitColsExplicitSelectV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT A, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("A", MinorType.VARCHAR)
          .addNullable("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  /**
   * Verify that implicit columns are recognized and populated. Sanity test
   * of just one implicit column. V3 uses non-nullable VARCHAR for file
   * metadata columns.
   */

  @Test
  public void testImplicitColsExplicitSelectV3() throws IOException {
    try {
      enableV3(true);
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
    } finally {
      resetV3();
    }
  }

  /**
   * Verify that implicit columns are recognized and populated. Sanity test
   * of just one implicit column. V2 uses nullable VARCHAR for file
   * metadata columns.
   */

  @Test
  public void testImplicitColWildcardV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", "foo", "bar", TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  /**
   * Verify that implicit columns are recognized and populated. Sanity test
   * of just one implicit column. V3 uses non-nullable VARCHAR for file
   * metadata columns.
   */

  @Test
  public void testImplicitColWildcardV3() throws IOException {
    try {
      enableV3(true);
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
    } finally {
      resetV3();
    }
  }

  @Test
  public void testColsWithWildcard() throws IOException {
    try {
      enableV3(false);
      doTestColsWithWildcard();
      enableV3(true);
      doTestColsWithWildcard();
    } finally {
      resetV3();
    }
  }

  private void doTestColsWithWildcard() throws IOException {
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
   * V2 does not allow explicit use of dir0, dir1, etc. columns for a non-partitioned
   * file. Treated as undefined nullable int columns.
   */

  @Test
  public void testPartitionColsExplicitV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT a, dir0, dir5 FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.INT)
          .addNullable("dir5", MinorType.INT)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow("10", null, null)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  /**
   * V3 allows the use of partition columns, even for a non-partitioned file.
   * The columns are null of type Nullable VARCHAR. This is area of Drill
   * is a bit murky: it seems reasonable to support partition columns consistently
   * rather than conditionally based on the structure of the input.
   */
  @Test
  public void testPartitionColsExplicitV3() throws IOException {
    try {
      enableV3(true);
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
    } finally {
      resetV3();
    }
  }

  @Test
  public void testDupColumn() throws IOException {
    try {
      enableV3(false);
      doTestDupColumn();
      enableV3(true);
      doTestDupColumn();
    } finally {
      resetV3();
    }
  }

  private void doTestDupColumn() throws IOException {
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

  // This test cannot be run for V2. The data gets corrupted and we get
  // internal errors.

  /**
   * Test that ragged rows result in the "missing" columns being filled
   * in with the moral equivalent of a null column for CSV: a blank string.
   */
  @Test
  public void testRaggedRowsV3() throws IOException {
    try {
      enableV3(true);
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
    } finally {
      resetV3();
    }
  }

  /**
   * Test partition expansion. Because the two files are read in the
   * same scan operator, the schema is consistent. See
   * {@link TestPartitionRace} for the multi-threaded race where all
   * hell breaks loose.
   * <p>
   * V2, since Drill 1.12, puts partition columns ahead of data columns.
   */
  @Test
  public void testPartitionExpansionV2() throws IOException {
    try {
      enableV3(false);

      String sql = "SELECT * FROM `dfs.data`.`%s`";
      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addNullable("dir0", MinorType.VARCHAR)
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .buildSchema();

      // Read the two batches.

      for (int i = 0; i < 2; i++) {
        assertTrue(iter.hasNext());
        RowSet rowSet = iter.next();

        // Figure out which record this is and test accordingly.

        RowSetReader reader = rowSet.reader();
        assertTrue(reader.next());
        String col2 = reader.scalar(1).getString();
        if (col2.equals("10")) {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(null, "10", "foo", "bar")
              .build();
          RowSetUtilities.verify(expected, rowSet);
        } else {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(NESTED_DIR, "20", "fred", "wilma")
              .build();
          RowSetUtilities.verify(expected, rowSet);
        }
      }
      assertFalse(iter.hasNext());
    } finally {
      resetV3();
    }
  }

  /**
   * Test partition expansion in V3.
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
  public void testPartitionExpansionV3() throws IOException {
    try {
      enableV3(true);

      String sql = "SELECT * FROM `dfs.data`.`%s`";
      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.VARCHAR)
          .buildSchema();

      // First batch is empty; just carries the schema.

      assertTrue(iter.hasNext());
      RowSet rowSet = iter.next();
      assertEquals(0, rowSet.rowCount());
      rowSet.clear();

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
    } finally {
      resetV3();
    }
  }

  /**
   * Test the use of partition columns with the wildcard. This works for file
   * metadata columns, but confuses the project operator when used for
   * partition columns. DRILL-7080.
   */
  @Test
  public void testWilcardAndPartitionsMultiFilesV2() throws IOException {
    try {
      enableV3(false);

      String sql = "SELECT *, dir0, dir1 FROM `dfs.data`.`%s`";
      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addNullable("dir0", MinorType.VARCHAR)
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("dir00", MinorType.VARCHAR)
          .addNullable("dir1", MinorType.INT)
          .buildSchema();

      // Read the two batches.

      for (int i = 0; i < 2; i++) {
        assertTrue(iter.hasNext());
        RowSet rowSet = iter.next();

        // Figure out which record this is and test accordingly.

        RowSetReader reader = rowSet.reader();
        assertTrue(reader.next());
        String aCol = reader.scalar("a").getString();
        if (aCol.equals("10")) {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(null, "10", "foo", "bar", null, null)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        } else {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(NESTED_DIR, "20", "fred", "wilma", NESTED_DIR, null)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        }
      }
      assertFalse(iter.hasNext());
    } finally {
      resetV3();
    }
  }

  /**
   * Test the use of partition columns with the wildcard. This works for file
   * metadata columns, but confuses the project operator when used for
   * partition columns. DRILL-7080. Still broken in V3 because this appears
   * to be a Project operator issue, not reader issue. Not that the
   * partition column moves after data columns.
   */
  @Test
  public void testWilcardAndPartitionsMultiFilesV3() throws IOException {
    try {
      enableV3(true);

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

      // First batch is empty; just carries the schema.

      assertTrue(iter.hasNext());
      RowSet rowSet = iter.next();
      RowSetUtilities.verify(new RowSetBuilder(client.allocator(), expectedSchema).build(),
          rowSet);

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
    } finally {
      resetV3();
    }
  }

  /**
   * Test using partition columns with partitioned files in V2. Since the
   * file is nested to one level, dir0 is a nullable VARCHAR, but dir1 is
   * a nullable INT. Since both files are read in a single scan operator,
   * the schema is consistent.
   */
  @Test
  public void doTestExplicitPartitionsMultiFilesV2() throws IOException {
    try {
      enableV3(false);

      String sql = "SELECT a, b, c, dir0, dir1 FROM `dfs.data`.`%s`";
      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.VARCHAR)
          .addNullable("dir1", MinorType.INT)
          .buildSchema();

      // Read the two batches.

      for (int i = 0; i < 2; i++) {
        assertTrue(iter.hasNext());
        RowSet rowSet = iter.next();

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
    } finally {
      resetV3();
    }
  }

  /**
   * Test using partition columns with partitioned files in V3. Although the
   * file is nested to one level, both dir0 and dir1 are nullable VARCHAR.
   * See {@link TestPartitionRace} to show that the types and schemas
   * are consistent even when used across multiple scans.
   */
  @Test
  public void doTestExplicitPartitionsMultiFilesV3() throws IOException {
    try {
      enableV3(true);

      String sql = "SELECT a, b, c, dir0, dir1 FROM `dfs.data`.`%s`";
      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .addNullable("dir0", MinorType.VARCHAR)
          .addNullable("dir1", MinorType.VARCHAR)
          .buildSchema();

      // First batch is empty; just carries the schema.

      assertTrue(iter.hasNext());
      RowSet rowSet = iter.next();
      RowSetUtilities.verify(new RowSetBuilder(client.allocator(), expectedSchema).build(),
          rowSet);

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
    finally {
      resetV3();
    }
  }

  /**
   * The column name `columns` is treated as a plain old
   * column when using column headers.
   */
  @Test
  public void testColumnsColV3() throws IOException {
    try {
      enableV3(true);

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
    } finally {
      resetV3();
    }
  }

  /**
   * The column name `columns` is treated as a plain old
   * column when using column headers. If used with an index,
   * validation will fail because the VarChar column is not an array
   */
  @Test
  public void testColumnsIndexV3() throws IOException {
    try {
      enableV3(true);

      String sql = "SELECT author, columns[0] FROM `dfs.data`.`%s`";
      client.queryBuilder().sql(sql, COLUMNS_FILE_NAME).run();
    } catch (UserRemoteException e) {
      assertTrue(e.getMessage().contains(
          "VALIDATION ERROR: Unexpected `columns`[x]; columns array not enabled"));
      assertTrue(e.getMessage().contains("Format plugin: text"));
      assertTrue(e.getMessage().contains("Plugin config name: csv"));
      assertTrue(e.getMessage().contains("Extract headers: true"));
      assertTrue(e.getMessage().contains("Skip headers: false"));
    } catch (Exception e) {
      fail();
    } finally {
      resetV3();
    }
  }

  @Test
  public void testColumnsMissingV3() throws IOException {
    try {
      enableV3(true);

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
    } finally {
      resetV3();
    }
  }

  /**
   * If columns[x] is used, then this can't possibly match a valid
   * text reader column, so raise an error instead.
   */
  @Test
  public void testColumnsIndexMissingV3() throws IOException {
    try {
      enableV3(true);

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
      assertTrue(e.getMessage().contains("Skip headers: false"));
    } catch (Exception e) {
      fail();
    } finally {
      resetV3();
    }
  }
}
