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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Demonstrates a race condition inherent in the way that partition
 * columns are currently implemented. Two files: one at the root directory,
 * one down one level. Parallelization is forced to two. (Most tests use
 * small files and both files end up being read in the same scanner, which
 * masks the problem shown here.)
 * <p>
 * Depending on which file is read first, the output row may start with
 * or without the partition column. Once the column occurs, it will
 * persist.
 * <p>
 * The solution is to figure out the max partition depth in the
 * EasySubScan rather than in each scan operator.
 * <p>
 * The tests here test both the "V2" (AKA "new text reader") which has
 * many issues, and the "V3" (row-set-based version) that has fixes.
 * <p>
 * See DRILL-7082 for the multi-scan race (fixed in V3), and
 * DRILL-7083 for the problem with partition columns returning nullable INT
 * (also fixed in V3.)
 */

public class TestPartitionRace extends BaseCsvTest {

  @BeforeClass
  public static void setup() throws Exception {
    BaseCsvTest.setup(false,  true, 2);

    // Two-level partitioned table

    File rootDir = new File(testDir, PART_DIR);
    rootDir.mkdir();
    buildFile(new File(rootDir, "first.csv"), validHeaders);
    File nestedDir = new File(rootDir, NESTED_DIR);
    nestedDir.mkdir();
    buildFile(new File(nestedDir, "second.csv"), secondFile);
  }

  /**
   * Oddly, when run in a single fragment, the files occur in a
   * stable order, the partition always appars, and it appears in
   * the first column position.
   */
  @Test
  public void testSingleScanV2() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";

    try {
      enableV3(false);

      // Loop to run the query 10 times, or until we see the race

      boolean sawMissingPartition = false;
      boolean sawPartitionFirst = false;
      boolean sawPartitionLast = false;

      // Read the two batches.

      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();
      for (int j = 0; j < 2; j++) {
        assertTrue(iter.hasNext());
        RowSet rowSet = iter.next();

        // Check location of partition column

        int posn = rowSet.schema().index("dir0");
        if (posn == -1) {
          sawMissingPartition = true;
        } else if (posn == 0) {
          sawPartitionFirst = true;
        } else {
          sawPartitionLast = true;
        }
        rowSet.clear();
      }
      assertFalse(iter.hasNext());

      // When run in a single fragment, the partition column appears
      // all the time, and is in the first column position.

      assertFalse(sawMissingPartition);
      assertTrue(sawPartitionFirst);
      assertFalse(sawPartitionLast);
    } finally {
      resetV3();
      client.resetSession(ExecConstants.MIN_READER_WIDTH_KEY);
    }
  }

  /**
   * V3 provides the same schema for the single- and multi-scan
   * cases.
   */
  @Test
  public void testSingleScanV3() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();

    try {
      enableV3(true);

      // Loop to run the query 10 times to verify no race

      // First batch is empty; just carries the schema.

      Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();
      assertTrue(iter.hasNext());
      RowSet rowSet = iter.next();
      assertEquals(0, rowSet.rowCount());
      rowSet.clear();

      // Read the two batches.

      for (int j = 0; j < 2; j++) {
        assertTrue(iter.hasNext());
        rowSet = iter.next();

        // Figure out which record this is and test accordingly.

        RowSetReader reader = rowSet.reader();
        assertTrue(reader.next());
        String col1 = reader.scalar("a").getString();
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
   * When forced to run in two fragments, the fun really starts. The
   * partition column (usually) appears in the last column position instead
   * of the first. The partition may or may not occur in the first row
   * depending on which file is read first. The result is that the
   * other columns will jump around. If we tried to create an expected
   * result set, we'd be frustrated because the schema randomly changes.
   * <p>
   * Just to be clear: this behavior is a bug, not a feature. But, it is
   * an established baseline for the "V2" reader.
   * <p>
   * This is really a test (demonstration) of the wrong behavior. This test
   * is pretty unreliable. In particular, the position of the partition column
   * seems to randomly shift from first to last position across runs.
   */
  @Test
  public void testRaceV2() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";

    try {
      enableV3(false);
      enableMultiScan();

      // Loop to run the query 10 times, or until we see the race

      boolean sawRootFirst = false;
      boolean sawNestedFirst = false;
      boolean sawMissingPartition = false;
      boolean sawPartitionFirst = false;
      boolean sawPartitionLast = false;
      for (int i = 0; i < 10; i++) {

        // Read the two batches.

        Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();
        for (int j = 0; j < 2; j++) {
          assertTrue(iter.hasNext());
          RowSet rowSet = iter.next();

          // Check location of partition column

          int posn = rowSet.schema().index("dir0");
          if (posn == -1) {
            sawMissingPartition = true;
          } else if (posn == 0) {
            sawPartitionFirst = true;
          } else {
            sawPartitionLast = true;
          }

          // Figure out which record this is and test accordingly.

          RowSetReader reader = rowSet.reader();
          assertTrue(reader.next());
          String col1 = reader.scalar("a").getString();
          if (col1.equals("10")) {
            if (i == 0) {
              sawRootFirst = true;
            }
          } else {
            if (i == 0) {
              sawNestedFirst = true;
            }
          }
          rowSet.clear();
        }
        assertFalse(iter.hasNext());
        if (sawMissingPartition &&
            sawPartitionFirst &&
            sawPartitionLast &&
            sawRootFirst &&
            sawNestedFirst) {
          // The following should appear most of the time.
          System.out.println("All variations occurred");
          return;
        }
      }

      // If you see this, maybe something got fixed. Or, maybe the
      // min parallelization hack above stopped working.
      // Or, you were just unlucky and can try the test again.
      // We print messages, rather than using assertTrue, to avoid
      // introducing a flaky test.

      System.out.println("Some variations did not occur");
      System.out.println(String.format("Missing partition: %s", sawMissingPartition));
      System.out.println(String.format("Partition first: %s", sawPartitionFirst));
      System.out.println(String.format("Partition last: %s", sawPartitionLast));
      System.out.println(String.format("Outer first: %s", sawRootFirst));
      System.out.println(String.format("Nested first: %s", sawNestedFirst));
    } finally {
      resetV3();
      resetMultiScan();
    }
  }

  /**
   * V3 computes partition depth in the group scan (which sees all files), and
   * so the partition column count does not vary across scans. Also, V3 puts
   * partition columns at the end of the row so that data columns don't
   * "jump around" when files are shifted to a new partition depth.
   */
  @Test
  public void testNoRaceV3() throws IOException {
    String sql = "SELECT * FROM `dfs.data`.`%s`";

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .add("b", MinorType.VARCHAR)
        .add("c", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();

    try {
      enableV3(true);
      enableMultiScan();

      // Loop to run the query 10 times or until we see both files
      // in the first position.

      boolean sawRootFirst = false;
      boolean sawNestedFirst = false;
      for (int i = 0; i < 10; i++) {

        // First batch is empty; just carries the schema.

        Iterator<DirectRowSet> iter = client.queryBuilder().sql(sql, PART_DIR).rowSetIterator();
        assertTrue(iter.hasNext());
        RowSet rowSet = iter.next();
        assertEquals(0, rowSet.rowCount());
        rowSet.clear();

        // Read the two batches.

        for (int j = 0; j < 2; j++) {
          assertTrue(iter.hasNext());
          rowSet = iter.next();

          // Figure out which record this is and test accordingly.

          RowSetReader reader = rowSet.reader();
          assertTrue(reader.next());
          String col1 = reader.scalar("a").getString();
          if (col1.equals("10")) {
            if (i == 0) {
              sawRootFirst = true;
            }
            RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
                .addRow("10", "foo", "bar", null)
                .build();
            RowSetUtilities.verify(expected, rowSet);
          } else {
            if (i == 0) {
              sawNestedFirst = true;
            }
            RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
                .addRow("20", "fred", "wilma", NESTED_DIR)
                .build();
            RowSetUtilities.verify(expected, rowSet);
          }
        }
        assertFalse(iter.hasNext());
        if (sawRootFirst &&
            sawNestedFirst) {
          // The following should appear most of the time.
          System.out.println("Both variations occurred");
          return;
        }
      }

      // If you see this, maybe something got fixed. Or, maybe the
      // min parallelization hack above stopped working.
      // Or, you were just unlucky and can try the test again.
      // We print messages, rather than using assertTrue, to avoid
      // introducing a flaky test.

      System.out.println("Some variations did not occur");
      System.out.println(String.format("Outer first: %s", sawRootFirst));
      System.out.println(String.format("Nested first: %s", sawNestedFirst));
    } finally {
      resetV3();
      resetMultiScan();
    }
  }
}
