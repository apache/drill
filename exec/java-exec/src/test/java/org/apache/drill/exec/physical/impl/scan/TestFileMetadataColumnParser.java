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
import static org.junit.Assert.fail;

import java.util.List;

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

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class TestFileMetadataColumnParser extends SubOperatorTest {

  @Test
  public void testBasics() {

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    // Simulate SELECT a, b, c ...

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("a", "b", "c"),
        Lists.newArrayList(metadataManager.projectionParser()));

    // Verify

    assertFalse(scanProj.projectAll());
    assertFalse(metadataManager.hasMetadata());
    assertTrue(metadataManager.useLegacyWildcardPartition());
  }

  /**
   * Test including file metadata (AKA "implicit columns") in the project
   * list.
   */

  @Test
  public void testFileMetadataColumnSelection() {

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(), true,
        new Path("hdfs:///w"),
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

    assertTrue(metadataManager.hasMetadata());
  }

  /**
   * Verify that partition columns, in any case, work.
   */

  @Test
  public void testPartitionColumnSelection() {

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(), true,
        new Path("hdfs:///w"),
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

  @Test
  public void testWildcard() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectAll(),
        Lists.newArrayList(metadataManager.projectionParser()));

    List<ColumnProjection> cols = scanProj.columns();
    assertEquals(7, cols.size());
    assertEquals(UnresolvedColumn.WILDCARD, cols.get(0).nodeType());
    for (int i = 0; i < 4; i++) {
      assertEquals(FileMetadataColumn.ID, cols.get(1+i).nodeType());
    }
    assertEquals(PartitionColumn.ID, cols.get(5).nodeType());
    assertEquals(PartitionColumn.ID, cols.get(6).nodeType());
  }

  /**
   * Drill 1.1 - 1.11 and Drill 1.13 or later put metadata columns after
   * data columns. Drill 1.12 moved them before data columns. For testing
   * and compatibility, the client can request to use the Drill 1.12 position,
   * though the after-data position is the default.
   */

  @Test
  public void testDrill1_12Wildcard() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectAll(),
        Lists.newArrayList(metadataManager.projectionParser()),
        true);

    List<ColumnProjection> cols = scanProj.columns();
    assertEquals(7, cols.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(FileMetadataColumn.ID, cols.get(i).nodeType());
    }
    assertEquals(PartitionColumn.ID, cols.get(4).nodeType());
    assertEquals(PartitionColumn.ID, cols.get(5).nodeType());
    assertEquals(UnresolvedColumn.WILDCARD, cols.get(6).nodeType());
  }

  /**
   * Can't explicitly list file metadata columns with a wildcard in
   * "legacy" mode: that is, when the wildcard already includes partition
   * and file metadata columns.
   */

  @Test
  public void testErrorWildcardLegacyAndFileMetaata() {

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    try {
      new ScanLevelProjection(
          RowSetTestUtils.projectList(ScanTestUtils.FILE_NAME_COL,
              SchemaPath.DYNAMIC_STAR),
          Lists.newArrayList(metadataManager.projectionParser()));
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * Can't include both a wildcard and a partition column.
   */

  @Test
  public void testErrorWildcardLegacyAndPartition() {

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    try {
      new ScanLevelProjection(
          RowSetTestUtils.projectList(SchemaPath.DYNAMIC_STAR,
              ScanTestUtils.partitionColName(8)),
          Lists.newArrayList(metadataManager.projectionParser()));
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
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
        fixture.getOptionManager(), true,
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(
            ScanTestUtils.FILE_NAME_COL + ".a",
            ScanTestUtils.FILE_PATH_COL + "[0]",
            ScanTestUtils.partitionColName(0) + ".b",
            ScanTestUtils.partitionColName(1) + "[0]",
            ScanTestUtils.SUFFIX_COL),
        Lists.newArrayList(metadataManager.projectionParser()),
        true);

    List<ColumnProjection> cols = scanProj.columns();
    assertEquals(5, cols.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(UnresolvedColumn.UNRESOLVED, cols.get(1).nodeType());
    }
    assertEquals(FileMetadataColumn.ID, cols.get(4).nodeType());
  }
}
