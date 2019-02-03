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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayManager;
import org.apache.drill.exec.physical.impl.scan.file.FileMetadataManager;
import org.apache.drill.exec.physical.impl.scan.project.ScanSchemaOrchestrator;
import org.apache.drill.exec.physical.impl.scan.project.ScanSchemaOrchestrator.ReaderSchemaOrchestrator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class TestColumnsArray extends SubOperatorTest {

  /**
   * Test columns array. The table must be able to support it by having a
   * matching column.
   */

  @Test
  public void testColumnsArray() {

    // Set up the file metadata manager

    Path filePath = new Path("hdfs:///w/x/y/z.csv");
    FileMetadataManager metadataManager = new FileMetadataManager(
        fixture.getOptionManager(),
        new Path("hdfs:///w"),
        Lists.newArrayList(filePath));

    // ...and the columns array manager

    ColumnsArrayManager colsManager = new ColumnsArrayManager(false);

    // Configure the schema orchestrator

    ScanSchemaOrchestrator scanner = new ScanSchemaOrchestrator(fixture.allocator());
    scanner.withMetadata(metadataManager);
    scanner.addParser(colsManager.projectionParser());
    scanner.addResolver(colsManager.resolver());

    // SELECT filename, columns, dir0 ...

    scanner.build(RowSetTestUtils.projectList(ScanTestUtils.FILE_NAME_COL,
        ColumnsArrayManager.COLUMNS_COL,
        ScanTestUtils.partitionColName(0)));

    // FROM z.csv

    metadataManager.startFile(filePath);
    ReaderSchemaOrchestrator reader = scanner.startReader();

    // Table schema (columns: VARCHAR[])

    TupleMetadata tableSchema = new SchemaBuilder()
        .addArray(ColumnsArrayManager.COLUMNS_COL, MinorType.VARCHAR)
        .buildSchema();

    ResultSetLoader loader = reader.makeTableLoader(tableSchema);

    // Verify empty batch.

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("filename", MinorType.VARCHAR)
        .addArray("columns", MinorType.VARCHAR)
        .addNullable("dir0", MinorType.VARCHAR)
        .buildSchema();
    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
         .build();

      assertNotNull(scanner.output());
      new RowSetComparison(expected)
         .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    // Create a batch of data.

    reader.startBatch();
    loader.writer()
      .addRow(new Object[] {new String[] {"fred", "flintstone"}})
      .addRow(new Object[] {new String[] {"barney", "rubble"}});
    reader.endBatch();

    // Verify

    {
      SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("z.csv", new String[] {"fred", "flintstone"}, "x")
        .addRow("z.csv", new String[] {"barney", "rubble"}, "x")
        .build();

      new RowSetComparison(expected)
          .verifyAndClearAll(fixture.wrap(scanner.output()));
    }

    scanner.close();
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
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    scanner.close();
  }
}
