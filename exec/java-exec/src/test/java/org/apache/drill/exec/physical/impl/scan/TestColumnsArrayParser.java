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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayManager;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayParser;
import org.apache.drill.exec.physical.impl.scan.columns.UnresolvedColumnsArrayColumn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataColumn;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.test.SubOperatorTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

@Category(RowSetTests.class)
public class TestColumnsArrayParser extends SubOperatorTest {

  /**
   * Test the special "columns" column that asks to return all columns
   * as an array. No need for early schema. This case is special: it actually
   * creates the one and only table column to match the desired output column.
   */

  @Test
  public void testColumnsArray() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(ColumnsArrayManager.COLUMNS_COL),
        ScanTestUtils.parsers(new ColumnsArrayParser(false)));

    assertFalse(scanProj.projectAll());
    assertEquals(1, scanProj.requestedCols().size());

    assertEquals(1, scanProj.columns().size());
    assertEquals(ColumnsArrayManager.COLUMNS_COL, scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(0).nodeType());
  }

  @Test
  public void testRequiredColumnsArray() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(ColumnsArrayManager.COLUMNS_COL),
        ScanTestUtils.parsers(new ColumnsArrayParser(true)));

    assertFalse(scanProj.projectAll());
    assertEquals(1, scanProj.requestedCols().size());

    assertEquals(1, scanProj.columns().size());
    assertEquals(ColumnsArrayManager.COLUMNS_COL, scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(0).nodeType());
  }

  @Test
  public void testRequiredWildcard() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectAll(),
        ScanTestUtils.parsers(new ColumnsArrayParser(true)));

    assertFalse(scanProj.projectAll());
    assertEquals(1, scanProj.requestedCols().size());

    assertEquals(1, scanProj.columns().size());
    assertEquals(ColumnsArrayManager.COLUMNS_COL, scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(0).nodeType());
  }

  @Test
  public void testColumnsArrayCaseInsensitive() {

    // Sic: case variation of standard name

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList("Columns"),
        ScanTestUtils.parsers(new ColumnsArrayParser(false)));

    assertFalse(scanProj.projectAll());
    assertEquals(1, scanProj.requestedCols().size());

    assertEquals(1, scanProj.columns().size());
    assertEquals("Columns", scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(0).nodeType());
  }

  @Test
  public void testColumnsElements() {

   ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(
            ColumnsArrayManager.COLUMNS_COL + "[3]",
            ColumnsArrayManager.COLUMNS_COL + "[1]"),
        ScanTestUtils.parsers(new ColumnsArrayParser(false)));

    assertFalse(scanProj.projectAll());
    assertEquals(2, scanProj.requestedCols().size());

    assertEquals(1, scanProj.columns().size());
    assertEquals(ColumnsArrayManager.COLUMNS_COL, scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(0).nodeType());
    UnresolvedColumnsArrayColumn colsCol = (UnresolvedColumnsArrayColumn) scanProj.columns().get(0);
    boolean indexes[] = colsCol.selectedIndexes();
    assertNotNull(indexes);
    assertEquals(4, indexes.length);
    assertFalse(indexes[0]);
    assertTrue(indexes[1]);
    assertFalse(indexes[0]);
    assertTrue(indexes[1]);
  }

  /**
   * The `columns` column is special; can't include both `columns` and
   * a named column in the same project.
   * <p>
   * TODO: This should only be true for text readers, make this an option.
   */

  @Test
  public void testErrorColumnsArrayAndColumn() {
    try {
      new ScanLevelProjection(
          RowSetTestUtils.projectList(ColumnsArrayManager.COLUMNS_COL, "a"),
          ScanTestUtils.parsers(new ColumnsArrayParser(false)));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  /**
   * Exclude a column and `columns` (reversed order of previous test).
   */

  @Test
  public void testErrorColumnAndColumnsArray() {
    try {
      new ScanLevelProjection(
          RowSetTestUtils.projectList("a", ColumnsArrayManager.COLUMNS_COL),
          ScanTestUtils.parsers(new ColumnsArrayParser(false)));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  /**
   * Can't request `columns` twice.
   */

  @Test
  public void testErrorTwoColumnsArray() {
    try {
      new ScanLevelProjection(
          RowSetTestUtils.projectList(ColumnsArrayManager.COLUMNS_COL, ColumnsArrayManager.COLUMNS_COL),
          ScanTestUtils.parsers(new ColumnsArrayParser(false)));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testErrorRequiredAndExtra() {
    try {
      new ScanLevelProjection(
          RowSetTestUtils.projectList("a"),
          ScanTestUtils.parsers(new ColumnsArrayParser(true)));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  @Test
  public void testColumnsIndexTooLarge() {
    try {
      new ScanLevelProjection(
          RowSetTestUtils.projectCols(SchemaPath.parseFromString("columns[70000]")),
          ScanTestUtils.parsers(new ColumnsArrayParser(true)));
      fail();
    } catch (UserException e) {
      // Expected
    }
  }

  /**
   * The `columns` column is special: can't be used with other column names.
   * Make sure that the rule <i>does not</i> apply to implicit columns.
   */

  @Test
  public void testMetadataColumnsWithColumnsArray() {
    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(ScanTestUtils.FILE_NAME_COL,
            ColumnsArrayManager.COLUMNS_COL,
            ScanTestUtils.SUFFIX_COL),
        ScanTestUtils.parsers(new ColumnsArrayParser(false),
            metadataManager.projectionParser()));

    assertFalse(scanProj.projectAll());

    assertEquals(3, scanProj.columns().size());

    assertEquals(ScanTestUtils.FILE_NAME_COL, scanProj.columns().get(0).name());
    assertEquals(ColumnsArrayManager.COLUMNS_COL, scanProj.columns().get(1).name());
    assertEquals(ScanTestUtils.SUFFIX_COL, scanProj.columns().get(2).name());

    // Verify column type

    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(0).nodeType());
    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(1).nodeType());
    assertEquals(FileMetadataColumn.ID, scanProj.columns().get(2).nodeType());
  }

  /**
   * If a query is of the form:
   * <pre><code>
   * select * from dfs.`multilevel/csv` where columns[1] < 1000
   * </code><pre>
   * Then the projection list passed to the scan operator
   * includes both the wildcard and the `columns` array.
   * We can ignore one of them.
   */

  @Test
  public void testWildcardAndColumns() {
    ScanLevelProjection scanProj = new ScanLevelProjection(
        RowSetTestUtils.projectList(
            SchemaPath.DYNAMIC_STAR,
            ColumnsArrayManager.COLUMNS_COL),
        ScanTestUtils.parsers(new ColumnsArrayParser(true)));

    assertFalse(scanProj.projectAll());
    assertEquals(2, scanProj.requestedCols().size());

    assertEquals(1, scanProj.columns().size());
    assertEquals(ColumnsArrayManager.COLUMNS_COL, scanProj.columns().get(0).name());

    // Verify column type

    assertEquals(UnresolvedColumnsArrayColumn.ID, scanProj.columns().get(0).nodeType());
  }

  @Test
  public void testColumnsAsMap() {
    try {
        new ScanLevelProjection(
          RowSetTestUtils.projectList("columns.x"),
          ScanTestUtils.parsers(new ColumnsArrayParser(true)));
        fail();
    }
    catch (UserException e) {
      // Expected
    }
  }
}
