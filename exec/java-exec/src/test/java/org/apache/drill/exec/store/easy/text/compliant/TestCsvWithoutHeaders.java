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

import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// CSV reader now hosted on the row set framework
@Category(RowSetTests.class)
public class TestCsvWithoutHeaders extends BaseCsvTest {

  private static final String TEST_FILE_NAME = "simple.csv";

  private static String sampleData[] = {
      "10,foo,bar",
      "20,fred,wilma"
  };

  private static String raggedRows[] = {
      "10,dino",
      "20,foo,bar",
      "30"
  };

  private static String secondSet[] = {
      "30,barney,betty"
  };

  @BeforeClass
  public static void setup() throws Exception {
    BaseCsvTest.setup(false,  false);

    buildFile(TEST_FILE_NAME, sampleData);
    buildNestedTableWithoutHeaders();
  }

  protected static void buildNestedTableWithoutHeaders() throws IOException {

    // Two-level partitioned table

    File rootDir = new File(testDir, PART_DIR);
    rootDir.mkdir();
    buildFile(new File(rootDir, ROOT_FILE), sampleData);
    File nestedDir = new File(rootDir, NESTED_DIR);
    nestedDir.mkdir();
    buildFile(new File(nestedDir, NESTED_FILE), secondSet);
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
   * Verify that the wildcard expands to the `columns` array
   */

  private void doTestWildcard() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addSingleCol(strArray("10", "foo", "bar"))
        .addSingleCol(strArray("20", "fred", "wilma"))
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testColumns() throws IOException {
    try {
      enableV3(false);
      doTestColumns();
      enableV3(true);
      doTestColumns();
    } finally {
      resetV3();
    }
  }

  private void doTestColumns() throws IOException {
    String sql = "SELECT columns FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addSingleCol(strArray("10", "foo", "bar"))
        .addSingleCol(strArray("20", "fred", "wilma"))
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void doTestWildcardAndMetadataV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addArray("columns", MinorType.VARCHAR)
          .addNullable("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow(strArray("10", "foo", "bar"), TEST_FILE_NAME)
          .addRow(strArray("20", "fred", "wilma"), TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void doTestWildcardAndMetadataV3() throws IOException {
    try {
      enableV3(true);
      String sql = "SELECT *, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addArray("columns", MinorType.VARCHAR)
          .add("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow(strArray("10", "foo", "bar"), TEST_FILE_NAME)
          .addRow(strArray("20", "fred", "wilma"), TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testColumnsAndMetadataV2() throws IOException {
    try {
      enableV3(false);
      String sql = "SELECT columns, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addArray("columns", MinorType.VARCHAR)
          .addNullable("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow(strArray("10", "foo", "bar"), TEST_FILE_NAME)
          .addRow(strArray("20", "fred", "wilma"), TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testColumnsAndMetadataV3() throws IOException {
    try {
      enableV3(true);
      String sql = "SELECT columns, filename FROM `dfs.data`.`%s`";
      RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

      TupleMetadata expectedSchema = new SchemaBuilder()
          .addArray("columns", MinorType.VARCHAR)
          .add("filename", MinorType.VARCHAR)
          .buildSchema();

      RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
          .addRow(strArray("10", "foo", "bar"), TEST_FILE_NAME)
          .addRow(strArray("20", "fred", "wilma"), TEST_FILE_NAME)
          .build();
      RowSetUtilities.verify(expected, actual);
    } finally {
      resetV3();
    }
  }

  @Test
  public void testSpecificColumns() throws IOException {
    try {
      enableV3(false);
      doTestSpecificColumns();
      enableV3(true);
      doTestSpecificColumns();
    } finally {
      resetV3();
    }
  }

  private void doTestSpecificColumns() throws IOException {
    String sql = "SELECT columns[0], columns[2] FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, TEST_FILE_NAME).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("EXPR$0", MinorType.VARCHAR)
        .addNullable("EXPR$1", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("10", "bar")
        .addRow("20", "wilma")
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testRaggedRows() throws IOException {
    String fileName = "ragged.csv";
    buildFile(fileName, raggedRows);
    try {
      enableV3(false);
      doTestRaggedRows(fileName);
      enableV3(true);
      doTestRaggedRows(fileName);
    } finally {
      resetV3();
    }
  }

  private void doTestRaggedRows(String fileName) throws IOException {
    String sql = "SELECT columns FROM `dfs.data`.`%s`";
    RowSet actual = client.queryBuilder().sql(sql, fileName).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addSingleCol(strArray("10", "dino"))
        .addSingleCol(strArray("20", "foo", "bar"))
        .addSingleCol(strArray("30"))
        .build();
    RowSetUtilities.verify(expected, actual);
  }

  /**
   * Test partition expansion. Because the two files are read in the
   * same scan operator, the schema is consistent.
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
          .addArray("columns", MinorType.VARCHAR)
          .buildSchema();

      // Read the two batches.

      for (int i = 0; i < 2; i++) {
        assertTrue(iter.hasNext());
        RowSet rowSet = iter.next();

        // Figure out which record this is and test accordingly.

        RowSetReader reader = rowSet.reader();
        assertTrue(reader.next());
        ArrayReader ar = reader.array(1);
        assertTrue(ar.next());
        String col1 = ar.scalar().getString();
        if (col1.equals("10")) {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(null, strArray("10", "foo", "bar"))
              .addRow(null, strArray("20", "fred", "wilma"))
              .build();
          RowSetUtilities.verify(expected, rowSet);
        } else {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(NESTED_DIR, strArray("30", "barney", "betty"))
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
          .addArray("columns", MinorType.VARCHAR)
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
        ArrayReader ar = reader.array(0);
        assertTrue(ar.next());
        String col1 = ar.scalar().getString();
        if (col1.equals("10")) {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(strArray("10", "foo", "bar"), null)
              .addRow(strArray("20", "fred", "wilma"), null)
              .build();
          RowSetUtilities.verify(expected, rowSet);
        } else {
          RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
              .addRow(strArray("30", "barney", "betty"), NESTED_DIR)
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
   * When the `columns` array is allowed, the projection list cannot
   * implicitly suggest that `columns` is a map.
   * <p>
   * V2 message: DATA_READ ERROR: Selected column 'columns' must be an array index
   */

  @Test
  public void testColumnsAsMap() throws IOException {
    String sql = "SELECT `%s`.columns.foo FROM `dfs.data`.`%s`";
    try {
      enableV3(true);
      client.queryBuilder().sql(sql, TEST_FILE_NAME, TEST_FILE_NAME).run();
    } catch (UserRemoteException e) {
      assertTrue(e.getMessage().contains(
          "VALIDATION ERROR: Column `columns` has map elements, but must be an array"));
      assertTrue(e.getMessage().contains("Plugin config name: csv"));
    } catch (Exception e) {
      fail();
    } finally {
      resetV3();
    }
  }
  /**
   * When the `columns` array is allowed, and an index is projected,
   * it must be below the maximum.
   * <p>
   * V2 message: INTERNAL_ERROR ERROR: 70000
   */

  @Test
  public void testColumnsIndexOverflow() throws IOException {
    String sql = "SELECT columns[70000] FROM `dfs.data`.`%s`";
    try {
      enableV3(true);
      client.queryBuilder().sql(sql, TEST_FILE_NAME, TEST_FILE_NAME).run();
    } catch (UserRemoteException e) {
      assertTrue(e.getMessage().contains(
          "VALIDATION ERROR: `columns`[70000] index out of bounds, max supported size is 65536"));
      assertTrue(e.getMessage().contains("Plugin config name: csv"));
    } catch (Exception e) {
      fail();
    } finally {
      resetV3();
    }
  }
}
