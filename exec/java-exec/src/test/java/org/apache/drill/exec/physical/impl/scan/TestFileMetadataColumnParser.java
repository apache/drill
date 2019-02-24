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
package org.apache.drill.exec.physical.impl.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.List;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager;
import org.apache.drill.exec.physical.impl.scan.file.PartitionColumn;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.UnresolvedColumn;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.test.SubOperatorTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

@Category(RowSetTests.class)
public class TestFileMetadataColumnParser extends SubOperatorTest {

  @Test
  public void testBasics() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        false, // Don't expand partiton columns for wildcard
        false, // N/A
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    // Simulate SELECT a, b, c ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a", "b", "c"),
        Lists.newArrayList(metadataManager.projectionParser()));

    // Verify

    assertFalse(scanProj.projectAll());
    assertFalse(metadataManager.hasImplicitCols());
  }

  /**
   * Test including file metadata (AKA "implicit columns") in the project
   * list.
   */

  @Test
  public void testFileMetadataColumnSelection() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        false, // Don't expand partition columns for wildcard
        false, // N/A
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    // Simulate SELECT a, fqn, filEPath, filename, suffix ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a",
            ScanTestUtils.FULLY_QUALIFIED_NAME_COL,
            "filEPath", // Sic, to test case sensitivity
            ScanTestUtils.FILE_NAME_COL,
            ScanTestUtils.SUFFIX_COL),
        Lists.newArrayList(metadataManager.projectionParser()));

    assertFalse(scanProj.projectAll());
    assertEquals(5, scanProj.requestedCols().size());

    assertEquals(5, scanProj.columns().size());

    assertEquals("a", scanProj.columns().get(0).name());
    assertEquals(ScanTestUtils.FULLY_QUALIFIED_NAME_COL, scanProj.columns().get(1).name());
    assertEquals("filEPath", scanProj.columns().get(2).name());
    assertEquals(ScanTestUtils.FILE_NAME_COL, scanProj.columns().get(3).name());
    assertEquals(ScanTestUtils.SUFFIX_COL, scanProj.columns().get(4).name());

    // Verify column type

    assertEquals(UnresolvedColumn.UNRESOLVED, scanProj.columns().get(0).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(1).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(2).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(3).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(4).nodeType());

    assertTrue(metadataManager.hasImplicitCols());
  }

  /**
   * Verify that partition columns, in any case, work.
   */

  @Test
  public void testPartitionColumnSelection() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        false, // Don't expand partition columns for wildcard
        false, // N/A
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    String dir0 = ScanTestUtils.partitionColName(0);
    // Sic: case insensitivity, but name in project list
    // is preferred over "natural" name.
    String dir1 = "DIR1";
    String dir2 = ScanTestUtils.partitionColName(2);
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(dir2, dir1, dir0, "a"),
        Lists.newArrayList(metadataManager.projectionParser()));

    assertEquals(4, scanProj.columns().size());
    assertEquals(dir2, scanProj.columns().get(0).name());
    assertEquals(dir1, scanProj.columns().get(1).name());
    assertEquals(dir0, scanProj.columns().get(2).name());
    assertEquals("a", scanProj.columns().get(3).name());

    // Verify column type

    assertEquals(PartitionColumn.ID, scanProj.columns().get(0).nodeType());
  }

  /**
   * Test wildcard expansion.
   */

  @Test
  public void testRevisedWildcard() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        false, // Don't expand partition columns for wildcard
        false, // N/A
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectAll(),
        Lists.newArrayList(metadataManager.projectionParser()));

    List<ColumnProjection> cols = scanProj.columns();
    assertEquals(1, cols.size());
    assertEquals(UnresolvedColumn.WILDCARD, cols.get(0).nodeType());
  }

  /**
   * Legacy (prior version) wildcard expansion always expands partition
   * columns.
   */

  @Test
  public void testLegacyWildcard() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        true, // Use legacy wildcard expansion
        true, // Put partitions at end
        new Path("hdfs:///w"),
        3, // Max partition depth is 3, though this "scan" sees only 2
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectAll(),
        Lists.newArrayList(metadataManager.projectionParser()));

    List<ColumnProjection> cols = scanProj.columns();
    assertEquals(4, cols.size());
    assertEquals(UnresolvedColumn.WILDCARD, cols.get(0).nodeType());
    assertEquals(PartitionColumn.ID, cols.get(1).nodeType());
    assertEquals(0, ((PartitionColumn) cols.get(1)).partition());
    assertEquals(PartitionColumn.ID, cols.get(2).nodeType());
    assertEquals(1, ((PartitionColumn) cols.get(2)).partition());
    assertEquals(PartitionColumn.ID, cols.get(3).nodeType());
    assertEquals(2, ((PartitionColumn) cols.get(3)).partition());
  }

  /**
   * Combine wildcard and file metadata columms. The wildcard expands
   * table columns but not metadata columns.
   */

  @Test
  public void testLegacyWildcardAndFileMetadata() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        true, // Use legacy wildcard expansion
        false, // Put partitions at end
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(
            SchemaPath.DYNAMIC_STAR,
            ScanTestUtils.FILE_NAME_COL,
            ScanTestUtils.SUFFIX_COL),
        Lists.newArrayList(metadataManager.projectionParser()));

    List<ColumnProjection> cols = scanProj.columns();
    assertEquals(5, cols.size());
    assertEquals(UnresolvedColumn.WILDCARD, cols.get(0).nodeType());
    assertEquals(FileMetadataColumn.ID, cols.get(1).nodeType());
    assertEquals(FileMetadataColumn.ID, cols.get(2).nodeType());
    assertEquals(PartitionColumn.ID, cols.get(3).nodeType());
    assertEquals(PartitionColumn.ID, cols.get(4).nodeType());
  }

  /**
   * As above, but include implicit columns before and after the
   * wildcard.
   */

  @Test
  public void testLegacyWildcardAndFileMetadataMixed() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        true, // Use legacy wildcard expansion
        false, // Put partitions at end
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(
            ScanTestUtils.FILE_NAME_COL,
            SchemaPath.DYNAMIC_STAR,
            ScanTestUtils.SUFFIX_COL),
        Lists.newArrayList(metadataManager.projectionParser()));

    List<ColumnProjection> cols = scanProj.columns();
    assertEquals(5, cols.size());
    assertEquals(FileMetadataColumn.ID, cols.get(0).nodeType());
    assertEquals(UnresolvedColumn.WILDCARD, cols.get(1).nodeType());
    assertEquals(FileMetadataColumn.ID, cols.get(2).nodeType());
    assertEquals(PartitionColumn.ID, cols.get(3).nodeType());
    assertEquals(PartitionColumn.ID, cols.get(4).nodeType());
  }

  /**
   * Include both a wildcard and a partition column. The wildcard, in
   * legacy mode, will create partition columns for any partitions not
   * mentioned in the project list.
   * <p>
   * Tests proposed functionality: included only requested partition
   * columns.
   */

  @Test
  public void testRevisedWildcardAndPartition() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        false, // Don't expand partition columns for wildcard
        false, // N/A
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR,
            ScanTestUtils.partitionColName(8)),
        Lists.newArrayList(metadataManager.projectionParser()));

      List<ColumnProjection> cols = scanProj.columns();
      assertEquals(2, cols.size());
      assertEquals(UnresolvedColumn.WILDCARD, cols.get(0).nodeType());
      assertEquals(PartitionColumn.ID, cols.get(1).nodeType());
  }

  @Test
  public void testLegacyWildcardAndPartition() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        true, // Use legacy wildcard expansion
        true, // Put partitions at end
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR,
            ScanTestUtils.partitionColName(8)),
        Lists.newArrayList(metadataManager.projectionParser()));

      List<ColumnProjection> cols = scanProj.columns();
      assertEquals(4, cols.size());
      assertEquals(UnresolvedColumn.WILDCARD, cols.get(0).nodeType());
      assertEquals(PartitionColumn.ID, cols.get(1).nodeType());
      assertEquals(0, ((PartitionColumn) cols.get(1)).partition());
      assertEquals(PartitionColumn.ID, cols.get(2).nodeType());
      assertEquals(1, ((PartitionColumn) cols.get(2)).partition());
      assertEquals(PartitionColumn.ID, cols.get(3).nodeType());
      assertEquals(8, ((PartitionColumn) cols.get(3)).partition());
  }

  @Test
  public void testPreferredPartitionExpansion() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        true, // Use legacy wildcard expansion
        false, // Put partitions at end
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR,
            ScanTestUtils.partitionColName(8)),
        Lists.newArrayList(metadataManager.projectionParser()));

      List<ColumnProjection> cols = scanProj.columns();
      assertEquals(4, cols.size());
      assertEquals(UnresolvedColumn.WILDCARD, cols.get(0).nodeType());
      assertEquals(PartitionColumn.ID, cols.get(1).nodeType());
      assertEquals(8, ((PartitionColumn) cols.get(1)).partition());
      assertEquals(PartitionColumn.ID, cols.get(2).nodeType());
      assertEquals(0, ((PartitionColumn) cols.get(2)).partition());
      assertEquals(PartitionColumn.ID, cols.get(3).nodeType());
      assertEquals(1, ((PartitionColumn) cols.get(3)).partition());
  }

  /**
   * Test a case like:<br>
   * <code>SELECT *, dir1 FROM ...</code><br>
   * The projection list includes "dir1". The wildcard will
   * fill in "dir0".
   */

  @Test
  public void testLegacyWildcardAndPartitionWithOverlap() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        true, // Use legacy wildcard expansion
        true, // Put partitions at end
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR,
            ScanTestUtils.partitionColName(1)),
        Lists.newArrayList(metadataManager.projectionParser()));

      List<ColumnProjection> cols = scanProj.columns();
      assertEquals(3, cols.size());
      assertEquals(UnresolvedColumn.WILDCARD, cols.get(0).nodeType());
      assertEquals(PartitionColumn.ID, cols.get(1).nodeType());
      assertEquals(0, ((PartitionColumn) cols.get(1)).partition());
      assertEquals(PartitionColumn.ID, cols.get(2).nodeType());
      assertEquals(1, ((PartitionColumn) cols.get(2)).partition());
  }

  @Test
  public void testPreferedWildcardExpansionWithOverlap() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        true, // Use legacy wildcard expansion
        false, // Put partitions at end
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR,
            ScanTestUtils.partitionColName(1)),
        Lists.newArrayList(metadataManager.projectionParser()));

      List<ColumnProjection> cols = scanProj.columns();
      assertEquals(3, cols.size());
      assertEquals(UnresolvedColumn.WILDCARD, cols.get(0).nodeType());
      assertEquals(PartitionColumn.ID, cols.get(1).nodeType());
      assertEquals(1, ((PartitionColumn) cols.get(1)).partition());
      assertEquals(PartitionColumn.ID, cols.get(2).nodeType());
      assertEquals(0, ((PartitionColumn) cols.get(2)).partition());
  }

  /**
   * Verify that names that look like metadata columns, but appear
   * to be maps or arrays, are not interpreted as metadata. That is,
   * the projected table map or array "shadows" the metadata column.
   */

  @Test
  public void testShadowed() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        false, // Don't expand partition columns for wildcard
        false, // N/A
        new Path("hdfs:///w"),
        FileMetadataManager.AUTO_PARTITION_DEPTH,
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(
            ScanTestUtils.FILE_NAME_COL + ".a",
            ScanTestUtils.FILE_PATH_COL + "[0]",
            ScanTestUtils.partitionColName(0) + ".b",
            ScanTestUtils.partitionColName(1) + "[0]",
            ScanTestUtils.SUFFIX_COL),
        Lists.newArrayList(metadataManager.projectionParser()));

    List<ColumnProjection> cols = scanProj.columns();
    assertEquals(5, cols.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(UnresolvedColumn.UNRESOLVED, cols.get(1).nodeType());
    }
    assertEquals(FileMetadataColumn.ID, cols.get(4).nodeType());
  }
}
