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
package org.apache.drill.exec.physical.rowSet.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests of the row limit functionality of the result set loader. The
 * row limit is set up front and has a default value. Because Drill must
 * discover data structure as it reads, the result set loader also allows changing
 * the row limit between batches (perhaps Drill discovers that rows are much
 * narrower or wider than expected.)
 * <p>
 * The tests here are independent of the tests for vector allocation (which does,
 * in fact, depend on the row count) and vector overflow (which an occur when
 * the row limit turns out to be too large.)
 */

@Category(RowSetTests.class)
public class TestResultSetLoaderLimits extends SubOperatorTest {

  /**
   * Verify that the writer stops when reaching the row limit.
   * In this case there is no look-ahead row.
   */

  @Test
  public void testRowLimit() {
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator());
    assertEquals(ResultSetLoaderImpl.DEFAULT_ROW_COUNT, rsLoader.targetRowCount());
    RowSetLoader rootWriter = rsLoader.writer();
    rootWriter.addColumn(SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REQUIRED));

    byte value[] = new byte[200];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    rsLoader.startBatch();
    while (! rootWriter.isFull()) {
      rootWriter.start();
      rootWriter.scalar(0).setBytes(value, value.length);
      rootWriter.save();
      count++;
    }
    assertEquals(ResultSetLoaderImpl.DEFAULT_ROW_COUNT, count);
    assertEquals(count, rootWriter.rowCount());

    rsLoader.harvest().clear();

    // Do it again, a different way.

    count = 0;
    rsLoader.startBatch();
    assertEquals(0, rootWriter.rowCount());
    while (rootWriter.start()) {
      rootWriter.scalar(0).setBytes(value, value.length);
      rootWriter.save();
      count++;
    }
    assertEquals(ResultSetLoaderImpl.DEFAULT_ROW_COUNT, count);
    assertEquals(count, rootWriter.rowCount());

    rsLoader.harvest().clear();

    rsLoader.close();
  }

  private static final int TEST_ROW_LIMIT = 1024;

  /**
   * Verify that the caller can set a row limit lower than the default.
   */

  @Test
  public void testCustomRowLimit() {

    // Try to set a default value larger than the hard limit. Value
    // is truncated to the limit.

    ResultSetOptions options = new OptionBuilder()
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT + 1)
        .build();
    assertEquals(ValueVector.MAX_ROW_COUNT, options.rowCountLimit);

    // Just a bit of paranoia that we check against the vector limit,
    // not any previous value...

    options = new OptionBuilder()
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT + 1)
        .setRowCountLimit(TEST_ROW_LIMIT)
        .build();
    assertEquals(TEST_ROW_LIMIT, options.rowCountLimit);

    options = new OptionBuilder()
        .setRowCountLimit(TEST_ROW_LIMIT)
        .setRowCountLimit(ValueVector.MAX_ROW_COUNT + 1)
        .build();
    assertEquals(ValueVector.MAX_ROW_COUNT, options.rowCountLimit);

    // Can't set the limit lower than 1

    options = new OptionBuilder()
        .setRowCountLimit(0)
        .build();
    assertEquals(1, options.rowCountLimit);

    // Do load with a (valid) limit lower than the default.

    options = new OptionBuilder()
        .setRowCountLimit(TEST_ROW_LIMIT)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    assertEquals(TEST_ROW_LIMIT, rsLoader.targetRowCount());

    RowSetLoader rootWriter = rsLoader.writer();
    rootWriter.addColumn(SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REQUIRED));

    rsLoader.startBatch();
    int count = fillToLimit(rootWriter);
    assertEquals(TEST_ROW_LIMIT, count);
    assertEquals(count, rootWriter.rowCount());

    // Should fail to write beyond the row limit

    assertFalse(rootWriter.start());
    try {
      rootWriter.save();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    rsLoader.harvest().clear();
    rsLoader.startBatch();
    assertEquals(0, rootWriter.rowCount());

    rsLoader.close();
  }

  private int fillToLimit(RowSetLoader rootWriter) {
    byte value[] = new byte[200];
    Arrays.fill(value, (byte) 'X');
    int count = 0;
    while (! rootWriter.isFull()) {
      rootWriter.start();
      rootWriter.scalar(0).setBytes(value, value.length);
      rootWriter.save();
      count++;
    }
    return count;
  }

  /**
   * Test that the row limit can change between batches.
   */

  @Test
  public void testDynamicLimit() {

    // Start with a small limit.

    ResultSetOptions options = new OptionBuilder()
        .setRowCountLimit(TEST_ROW_LIMIT)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
    assertEquals(TEST_ROW_LIMIT, rsLoader.targetRowCount());

    RowSetLoader rootWriter = rsLoader.writer();
    rootWriter.addColumn(SchemaBuilder.columnSchema("s", MinorType.VARCHAR, DataMode.REQUIRED));

    rsLoader.startBatch();
    int count = fillToLimit(rootWriter);
    assertEquals(TEST_ROW_LIMIT, count);
    assertEquals(count, rootWriter.rowCount());
    rsLoader.harvest().clear();

    // Reset the batch size larger and fill a second batch

    int newLimit = 8000;
    rsLoader.setTargetRowCount(newLimit);
    rsLoader.startBatch();
    count = fillToLimit(rootWriter);
    assertEquals(newLimit, count);
    assertEquals(count, rootWriter.rowCount());
    rsLoader.harvest().clear();

    // Put the limit back to a lower number.

    newLimit = 1000;
    rsLoader.setTargetRowCount(newLimit);
    rsLoader.startBatch();
    count = fillToLimit(rootWriter);
    assertEquals(newLimit, count);
    assertEquals(count, rootWriter.rowCount());
    rsLoader.harvest().clear();

    rsLoader.close();
  }
}
