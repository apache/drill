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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayManager;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager;
import org.apache.drill.exec.physical.impl.scan.project.ReaderSchemaOrchestrator;
import org.apache.drill.exec.physical.impl.scan.project.ScanSchemaOrchestrator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

/**
 * Test the "columns" array mechanism integrated with the scan schema
 * orchestrator including simulating reading data.
 */

@Category(RowSetTests.class)
public class TestColumnsArray extends SubOperatorTest {

  private static class MockScanner {
    ScanSchemaOrchestrator scanner;
    ReaderSchemaOrchestrator reader;
    ResultSetLoader loader;
  }

  private MockScanner buildScanner(List<SchemaPath> projList) {

    MockScanner mock = new MockScanner();

    // Set up the file metadata manager

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    // ...and the columns array manager

    ColumnsArrayManager colsManager = new ColumnsArrayManager(false);

    // Configure the schema orchestrator

    mock.scanner = new ScanSchemaOrchestrator(fixture.allocator());
    mock.scanner.withMetadata(metadataManager);
    mock.scanner.addParser(colsManager.projectionParser());
    mock.scanner.addResolver(colsManager.resolver());

    // SELECT <proj list> ...

    mock.scanner.build(projList);

    // FROM z.csv

    metadataManager.startFile(filePath);
    mock.reader = mock.scanner.startReader();

    // Table schema (columns: VARCHAR[])

    TupleMetadata tableSchema = new SchemaBuilder()
        .addArray(ColumnsArrayManager.COLUMNS_COL, MinorType.VARCHAR)
        .buildSchema();

    mock.loader = mock.reader.makeTableLoader(tableSchema);

    // First empty batch

    mock.reader.defineSchema();
    return mock;
  }

  /**
   * Test columns array. The table must be able to support it by having a
   * matching column.
   */

  @Test
  public void testColumnsArray() {

    MockScanner mock = buildScanner(RowSetTestUtils.projectList(ScanTestUtils.FILE_NAME_COL,
        ColumnsArrayManager.COLUMNS_COL,
        ScanTestUtils.partitionColName(0)));

    // Verify empty batch.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("filename", MinorType.VARCHAR)
        .addArray("columns", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
         .build();

      assertNotNull(mock.scanner.output());
      RowSetUtilities.verify(expected,
         fixture.wrap(mock.scanner.output()));
    }

    // Create a batch of data.

    mock.reader.startBatch();
    mock.loader.writer()
      .addRow(new Object[] {new String[] {"fred", "flintstone"}})
      .addRow(new Object[] {new String[] {"barney", "rubble"}});
    mock. reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("z.csv", new String[] {"fred", "flintstone"}, "x")
        .addRow("z.csv", new String[] {"barney", "rubble"}, "x")
        .build();

      RowSetUtilities.verify(expected,
          fixture.wrap(mock.scanner.output()));
    }

    mock.scanner.close();
  }

  @Test
  public void testWildcard() {

    MockScanner mock = buildScanner(RowSetTestUtils.projectAll());

    // Verify empty batch.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
         .build();

      assertNotNull(mock.scanner.output());
      RowSetUtilities.verify(expected,
         fixture.wrap(mock.scanner.output()));
    }

    // Create a batch of data.

    mock.reader.startBatch();
    mock.loader.writer()
      .addRow(new Object[] {new String[] {"fred", "flintstone"}})
      .addRow(new Object[] {new String[] {"barney", "rubble"}});
    mock. reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(new String[] {"fred", "flintstone"})
        .addSingleCol(new String[] {"barney", "rubble"})
        .build();

      RowSetUtilities.verify(expected,
          fixture.wrap(mock.scanner.output()));
    }

    mock.scanner.close();
  }

  @Test
  public void testWildcardAndFileMetadata() {

    MockScanner mock = buildScanner(RowSetTestUtils.projectList(
        ScanTestUtils.FILE_NAME_COL,
        SchemaPath.DYNAMIC_STAR,
        ScanTestUtils.partitionColName(0)));

    // Verify empty batch.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("filename", MinorType.VARCHAR)
        .addArray("columns", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
         .build();

      assertNotNull(mock.scanner.output());
      RowSetUtilities.verify(expected,
         fixture.wrap(mock.scanner.output()));
    }

    // Create a batch of data.

    mock.reader.startBatch();
    mock.loader.writer()
      .addRow(new Object[] {new String[] {"fred", "flintstone"}})
      .addRow(new Object[] {new String[] {"barney", "rubble"}});
    mock. reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("z.csv", new String[] {"fred", "flintstone"}, "x")
        .addRow("z.csv", new String[] {"barney", "rubble"}, "x")
        .build();

      RowSetUtilities.verify(expected,
          fixture.wrap(mock.scanner.output()));
    }

    mock.scanner.close();
  }

  private ScanSchemaOrchestrator buildScan(List<SchemaPath> cols) {

    // Set up the columns array manager

    ColumnsArrayManager colsManager = new ColumnsArrayManager(false);

    // Configure the schema orchestrator

    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());
    scanner.addParser(colsManager.projectionParser());
    scanner.addResolver(colsManager.resolver());

    scanner.build(cols);
    return scanner;
  }

  /**
   * Test attempting to use the columns array with an early schema with
   * column types not compatible with a varchar array.
   */

  @Test
  public void testMissingColumnsColumn() {
    ScanSchemaOrchestrator scanner = buildScan(
        RowSetTestUtils.projectList(ColumnsArrayManager.COLUMNS_COL));

    TupleMetadata tableSchema = new SchemaBuilder()
        .add("a", MinorType.VARCHAR)
        .buildSchema();

    try {
      ReaderSchemaOrchestrator reader = scanner.startReader();
      reader.makeTableLoader(tableSchema);
      reader.defineSchema();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    scanner.close();
  }

  @Test
  public void testNotRepeated() {
    ScanSchemaOrchestrator scanner = buildScan(
        RowSetTestUtils.projectList(ColumnsArrayManager.COLUMNS_COL));

    TupleMetadata tableSchema = new SchemaBuilder()
        .add(ColumnsArrayManager.COLUMNS_COL, MinorType.VARCHAR)
        .buildSchema();

    try {
      ReaderSchemaOrchestrator reader = scanner.startReader();
      reader.makeTableLoader(tableSchema);
      reader.defineSchema();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    scanner.close();
  }
}
