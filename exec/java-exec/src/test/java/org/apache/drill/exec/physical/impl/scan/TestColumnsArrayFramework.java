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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.TestFileScanFramework.BaseFileScanOpFixture;
import org.apache.drill.exec.physical.impl.scan.TestFileScanFramework.DummyFileWork;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayManager;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsScanFramework;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsScanFramework.FileReaderCreator;
import org.apache.drill.exec.physical.impl.scan.file.BaseFileScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.test.SubOperatorTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;


/**
 * Test the columns-array specific behavior in the columns scan framework.
 */

@Category(RowSetTests.class)
public class TestColumnsArrayFramework extends SubOperatorTest {

  private static final Path MOCK_FILE_PATH = new Path("file:/w/x/y/z.csv");

  public static class ColumnsScanOpFixture extends BaseFileScanOpFixture implements FileReaderCreator {

    protected final List<DummyColumnsReader> readers = new ArrayList<>();
    protected Iterator<DummyColumnsReader> readerIter;

    public void addReader(DummyColumnsReader reader) {
      readers.add(reader);
      files.add(new DummyFileWork(reader.filePath()));
    }

    @Override
    protected BaseFileScanFramework<?> buildFramework() {
      readerIter = readers.iterator();
      return new ColumnsScanFramework(projection, files, fsConfig, this);
    }

    @Override
    public ManagedReader<ColumnsSchemaNegotiator> makeBatchReader(
        DrillFileSystem dfs,
        FileSplit split) throws ExecutionSetupException {
      if (! readerIter.hasNext()) {
        return null;
      }
      return readerIter.next();
    }
  }

  public static class DummyColumnsReader implements ManagedReader<ColumnsSchemaNegotiator> {

    public TupleMetadata schema;
    public ColumnsSchemaNegotiator negotiator;
    int nextCount;
    protected Path filePath = MOCK_FILE_PATH;

    public DummyColumnsReader(TupleMetadata schema) {
      this.schema = schema;
    }

    public Path filePath() { return filePath; }

    @Override
    public boolean open(ColumnsSchemaNegotiator negotiator) {
      this.negotiator = negotiator;
      negotiator.setTableSchema(schema, true);
      negotiator.build();
      return true;
    }

    @Override
    public boolean next() {
      return nextCount++ == 0;
    }

    @Override
    public void close() { }
  }

  /**
   * Test including a column other than "columns". Occurs when
   * using implicit columns.
   */

  @Test
  public void testNonColumnsProjection() {

    // Create a mock reader, return two batches: one schema-only, another with data.

    DummyColumnsReader reader = new DummyColumnsReader(
        new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .buildSchema());

    // Create the scan operator

    ColumnsScanOpFixture scanFixture = new ColumnsScanOpFixture();
    scanFixture.projectAll();
    scanFixture.addReader(reader);
    ScanOperatorExec scan = scanFixture.build();

    // Start the one and only reader, and check the columns
    // schema info.

    assertTrue(scan.buildSchema());
    assertNotNull(reader.negotiator);
    assertFalse(reader.negotiator.columnsArrayProjected());
    assertNull(reader.negotiator.projectedIndexes());

    scanFixture.close();
  }

  /**
   * Test projecting just the `columns` column.
   */

  @Test
  public void testColumnsProjection() {

    // Create a mock reader, return two batches: one schema-only, another with data.

    DummyColumnsReader reader = new DummyColumnsReader(
        new SchemaBuilder()
        .addArray(ColumnsArrayManager.COLUMNS_COL, MinorType.VARCHAR)
        .buildSchema());

    // Create the scan operator

    ColumnsScanOpFixture scanFixture = new ColumnsScanOpFixture();
    scanFixture.setProjection(RowSetTestUtils.projectList(ColumnsArrayManager.COLUMNS_COL));
    scanFixture.addReader(reader);
    ScanOperatorExec scan = scanFixture.build();

    // Start the one and only reader, and check the columns
    // schema info.

    assertTrue(scan.buildSchema());
    assertNotNull(reader.negotiator);
    assertTrue(reader.negotiator.columnsArrayProjected());
    assertNull(reader.negotiator.projectedIndexes());

    scanFixture.close();
  }

  /**
   * Test including a specific index of `columns` such as
   * `columns`[1].
   */
  @Test
  public void testColumnsIndexProjection() {

    // Create a mock reader, return two batches: one schema-only, another with data.

    DummyColumnsReader reader = new DummyColumnsReader(
        new SchemaBuilder()
        .addArray(ColumnsArrayManager.COLUMNS_COL, MinorType.VARCHAR)
        .buildSchema());

    // Create the scan operator

    ColumnsScanOpFixture scanFixture = new ColumnsScanOpFixture();
    scanFixture.setProjection(Lists.newArrayList(
        SchemaPath.parseFromString(ColumnsArrayManager.COLUMNS_COL + "[1]"),
        SchemaPath.parseFromString(ColumnsArrayManager.COLUMNS_COL + "[3]")));
    scanFixture.addReader(reader);
    ScanOperatorExec scan = scanFixture.build();

    // Start the one and only reader, and check the columns
    // schema info.

    assertTrue(scan.buildSchema());
    assertNotNull(reader.negotiator);
    assertTrue(reader.negotiator.columnsArrayProjected());
    boolean projIndexes[] = reader.negotiator.projectedIndexes();
    assertNotNull(projIndexes);
    assertEquals(4, projIndexes.length);
    assertFalse(projIndexes[0]);
    assertTrue(projIndexes[1]);
    assertFalse(projIndexes[2]);
    assertTrue(projIndexes[3]);

    scanFixture.close();
  }
}
