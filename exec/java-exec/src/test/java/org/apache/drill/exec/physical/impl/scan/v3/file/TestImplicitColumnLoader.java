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
package org.apache.drill.exec.physical.impl.scan.v3.file;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.apache.drill.categories.EvfTests;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils;
import org.apache.drill.exec.physical.impl.scan.v3.lifecycle.StaticBatchBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.lifecycle.BaseTestScanLifecycle.DummySubScan;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanSchemaConfigBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanSchemaTracker;
import org.apache.drill.exec.physical.resultSet.ResultVectorCache;
import org.apache.drill.exec.physical.resultSet.impl.NullResultVectorCacheImpl;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the file implicit column handler which identifies implicit columns
 * and populates them. Assumes that the implicit column parser tests pass.
 */
@Category(EvfTests.class)
public class TestImplicitColumnLoader extends SubOperatorTest implements MockFileNames {

  public StaticBatchBuilder buildHandler(List<SchemaPath> projList, Path root, Path input) {
    FileScanLifecycleBuilder options = new FileScanLifecycleBuilder();
    options.rootDir(root);
    final ResultVectorCache cache = new NullResultVectorCacheImpl(fixture.allocator());
    final ScanSchemaTracker schemaTracker = new ScanSchemaConfigBuilder()
        .projection(projList)
        .build();
    ImplicitFileColumnsHandler handler = new ImplicitFileColumnsHandler(
        fixture.operatorContext(new DummySubScan()), options, cache, schemaTracker);
    return handler.forFile(input);
  }

  @Test
  public void testNoColumns() {
    assertNull(buildHandler(RowSetTestUtils.projectNone(), MOCK_ROOT_PATH, MOCK_FILE_PATH));
  }

  @Test
  public void testOneColumn() {
    StaticBatchBuilder batchLoader = buildHandler(
        RowSetTestUtils.projectList("a", ScanTestUtils.FILE_NAME_COL, "b"),
        MOCK_ROOT_PATH, MOCK_FILE_PATH);
    assertNotNull(batchLoader);
    batchLoader.load(2);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add(ScanTestUtils.FILE_NAME_COL, IMPLICIT_COL_TYPE)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(MOCK_FILE_NAME)
        .addRow(MOCK_FILE_NAME)
        .build();
    RowSetUtilities.verify(expected, fixture.wrap(batchLoader.outputContainer()));
  }

  @Test
  public void testAllColumns() {
    StaticBatchBuilder batchLoader = buildHandler(
        ScanTestUtils.projectAllWithMetadata(3),
        MOCK_ROOT_PATH, MOCK_FILE_PATH);
    assertNotNull(batchLoader);
    batchLoader.load(2);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add(ScanTestUtils.FULLY_QUALIFIED_NAME_COL, IMPLICIT_COL_TYPE)
        .add(ScanTestUtils.FILE_PATH_COL, IMPLICIT_COL_TYPE)
        .add(ScanTestUtils.FILE_NAME_COL, IMPLICIT_COL_TYPE)
        .add(ScanTestUtils.SUFFIX_COL, IMPLICIT_COL_TYPE)
        .add(ScanTestUtils.partitionColName(0), PARTITION_COL_TYPE)
        .add(ScanTestUtils.partitionColName(1), PARTITION_COL_TYPE)
        .add(ScanTestUtils.partitionColName(2), PARTITION_COL_TYPE)
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(MOCK_FILE_FQN, MOCK_FILE_DIR_PATH, MOCK_FILE_NAME, MOCK_SUFFIX, MOCK_DIR0, MOCK_DIR1, null)
        .addRow(MOCK_FILE_FQN, MOCK_FILE_DIR_PATH, MOCK_FILE_NAME, MOCK_SUFFIX, MOCK_DIR0, MOCK_DIR1, null)
        .build();
    RowSetUtilities.verify(expected, fixture.wrap(batchLoader.outputContainer()));
  }
}
