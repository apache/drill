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

import java.util.Arrays;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ScanFixture;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.project.ScanSchemaOrchestrator;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test vector overflow in the context of the scan operator.
 */
@Category(RowSetTests.class)
public class TestScanOperExecOverflow extends BaseScanOperatorExecTest {

  /**
   * Mock reader that produces "jumbo" batches that cause a vector to
   * fill and a row to overflow from one batch to the next.
   */

  private static class OverflowReader extends BaseMockBatchReader {

    private final String value;
    public int rowCount;
    /**
     * If true, the reader will report EOF after filling a batch
     * to overflow. This simulates the corner case in which a reader
     * has, say, 1000 rows, hits overflow on row 1000, then declares
     * it has nothing more to read.
     * <p>
     * If false, reports EOF on a call to next() without reading more
     * rows. The overlow row from the prior batch still exists in
     * the result set loader.
     */
    public boolean reportEofWithOverflow;

    public OverflowReader() {
      char buf[] = new char[512];
      Arrays.fill(buf, 'x');
      value = new String(buf);
    }

    @Override
    public boolean open(SchemaNegotiator schemaNegotiator) {
      openCalled = true;
      TupleMetadata schema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .buildSchema();
      schemaNegotiator.tableSchema(schema, true);
      tableLoader = schemaNegotiator.build();
      return true;
    }

    @Override
    public boolean next() {
      batchCount++;
      if (batchCount > batchLimit) {
        return false;
      }

      RowSetLoader writer = tableLoader.writer();
      while (! writer.isFull()) {
        writer.start();
        writer.scalar(0).setString(value);
        writer.save();
        rowCount++;
      }

      // The vector overflowed on the last row. But, we still had to write the row.
      // The row is tucked away in the loader to appear as the first row in
      // the next batch.
      //
      // Depending on the flag set by the test routine, either report the EOF
      // during this read, or report it next time around.

      return reportEofWithOverflow
          ? batchCount < batchLimit
          : true;
    }
  }

  /**
   * Test multiple readers, with one of them creating "jumbo" batches
   * that overflow. Specifically, test a corner case. A batch ends right
   * at file EOF, but that last batch overflowed.
   */

  @Test
  public void testMultipleReadersWithOverflow() {
    runOverflowTest(false);
    runOverflowTest(true);
  }

  private void runOverflowTest(boolean eofWithData) {
    OverflowReader reader1 = new OverflowReader();
    reader1.batchLimit = 2;
    reader1.reportEofWithOverflow = eofWithData;
    MockEarlySchemaReader reader2 = new MockEarlySchemaReader();
    reader2.batchLimit = 2;

    BaseScanFixtureBuilder builder = new BaseScanFixtureBuilder();
    builder.projectAll();
    builder.addReader(reader1);
    builder.addReader(reader2);

    // Want overflow, set size and row counts at their limits.

    builder.builder.batchByteLimit(ScanSchemaOrchestrator.MAX_BATCH_BYTE_SIZE);
    builder.builder.batchRecordLimit(ScanSchemaOrchestrator.MAX_BATCH_ROW_COUNT);
    ScanFixture scanFixture = builder.build();
    ScanOperatorExec scan = scanFixture.scanOp;

    assertTrue(scan.buildSchema());
    assertEquals(1, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // Second batch. Should be 1 less than the reader's row
    // count because the loader has its own one-row lookahead batch.

    assertTrue(scan.next());
    assertEquals(1, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    int prevRowCount = scan.batchAccessor().rowCount();
    assertEquals(reader1.rowCount - 1, prevRowCount);
    scan.batchAccessor().release();

    // Third batch, adds more data to the lookahead batch. Also overflows
    // so returned records is one less than total produced so far minus
    // those returned earlier.

    assertTrue(scan.next());
    assertEquals(2, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    assertEquals(reader1.rowCount - prevRowCount - 1, scan.batchAccessor().rowCount());
    scan.batchAccessor().release();
    int prevReaderRowCount = reader1.rowCount;

    // Third batch. Returns the overflow row from the second batch of
    // the first reader.

    assertTrue(scan.next());
    assertEquals(eofWithData ? 2 : 3, reader1.batchCount);
    assertEquals(1, scan.batchAccessor().schemaVersion());
    assertEquals(1, scan.batchAccessor().rowCount());
    assertEquals(prevReaderRowCount, reader1.rowCount);
    scan.batchAccessor().release();

    // Second reader.

    assertTrue(scan.next());
    assertEquals(2, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // Second batch from second reader.

    assertTrue(scan.next());
    assertEquals(2, reader2.batchCount);
    assertEquals(2, scan.batchAccessor().schemaVersion());
    scan.batchAccessor().release();

    // EOF

    assertFalse(scan.next());
    assertEquals(0, scan.batchAccessor().rowCount());
    scanFixture.close();
  }

}
