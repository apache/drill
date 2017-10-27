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
package org.apache.drill.test.rowSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;
import org.bouncycastle.util.Arrays;

/**
 * For testing, compare the contents of two row sets (record batches)
 * to verify that they are identical. Supports masks to exclude certain
 * columns from comparison.
 */

public class RowSetComparison {

  private RowSet expected;
  private boolean mask[];
  private double delta = 0.001;
  private int offset;
  private int span = -1;

  public RowSetComparison(RowSet expected) {
    this.expected = expected;
    mask = new boolean[expected.schema().hierarchicalAccess().count()];
    for (int i = 0; i < mask.length; i++) {
      mask[i] = true;
    }
  }

  /**
   * Mark a specific column as excluded from comparisons.
   * @param colNo the index of the column to exclude
   * @return this builder
   */

  public RowSetComparison exclude(int colNo) {
    mask[colNo] = false;
    return this;
  }

  /**
   * Specifies a "selection" mask that determines which columns
   * to compare. Columns marked as "false" are omitted from the
   * comparison.
   *
   * @param flags variable-length list of column flags
   * @return this builder
   */
  public RowSetComparison withMask(Boolean...flags) {
    for (int i = 0; i < flags.length; i++) {
      mask[i] = flags[i];
    }
    return this;
  }

  /**
   * Specify the delta value to use when comparing float or
   * double values.
   *
   * @param delta the delta to use in float and double comparisons
   * @return this builder
   */
  public RowSetComparison withDelta(double delta) {
    this.delta = delta;
    return this;
  }

  /**
   * Specify an offset into the row sets to start the comparison.
   * Usually combined with {@link #span()}.
   *
   * @param offset offset into the row set to start the comparison
   * @return this builder
   */
  public RowSetComparison offset(int offset) {
    this.offset = offset;
    return this;
  }

  /**
   * Specify a subset of rows to compare. Usually combined
   * with {@link #offset()}.
   *
   * @param span the number of rows to compare
   * @return this builder
   */

  public RowSetComparison span(int span) {
    this.span = span;
    return this;
  }

  /**
   * Verify the actual rows using the rules defined in this builder
   * @param actual the actual results to verify
   */

  public void verify(RowSet actual) {
    int testLength = expected.rowCount() - offset;
    if (span > -1) {
      testLength = span;
    }
    int dataLength = offset + testLength;
    assertTrue("Missing expected rows", expected.rowCount() >= dataLength);
    assertTrue("Missing actual rows", actual.rowCount() >= dataLength);
    RowSetReader er = expected.reader();
    RowSetReader ar = actual.reader();
    for (int i = 0; i < offset; i++) {
      er.next();
      ar.next();
    }
    for (int i = 0; i < testLength; i++) {
      er.next();
      ar.next();
      verifyRow(er, ar);
    }
  }

  /**
   * Convenience method to verify the actual results, then free memory
   * for the actual result sets.
   * @param actual the actual results to verify
   */

  public void verifyAndClear(RowSet actual) {
    try {
      verify(actual);
    } finally {
      actual.clear();
    }
  }

  /**
   * Convenience method to verify the actual results, then free memory
   * for both the expected and actual result sets.
   * @param actual the actual results to verify
   */

  public void verifyAndClearAll(RowSet actual) {
    try {
      verify(actual);
    } finally {
      expected.clear();
      actual.clear();
    }
  }

  private void verifyRow(RowSetReader er, RowSetReader ar) {
    for (int i = 0; i < mask.length; i++) {
      if (! mask[i]) {
        continue;
      }
      ColumnReader ec = er.column(i);
      ColumnReader ac = ar.column(i);
      String label = (er.index() + 1) + ":" + i;
      assertEquals(label, ec.valueType(), ac.valueType());
      if (ec.isNull()) {
        assertTrue(label + " - column not null", ac.isNull());
        continue;
      }
      if (! ec.isNull()) {
        assertTrue(label + " - column is null", ! ac.isNull());
      }
    switch (ec.valueType()) {
    case BYTES: {
        byte expected[] = ac.getBytes();
        byte actual[] = ac.getBytes();
        assertEquals(label + " - byte lengths differ", expected.length, actual.length);
        assertTrue(label, Arrays.areEqual(expected, actual));
        break;
     }
     case DOUBLE:
       assertEquals(label, ec.getDouble(), ac.getDouble(), delta);
       break;
     case INTEGER:
       assertEquals(label, ec.getInt(), ac.getInt());
       break;
     case LONG:
       assertEquals(label, ec.getLong(), ac.getLong());
       break;
     case STRING:
       assertEquals(label, ec.getString(), ac.getString());
        break;
     case DECIMAL:
       assertEquals(label, ec.getDecimal(), ac.getDecimal());
       break;
     case PERIOD:
       assertEquals(label, ec.getPeriod(), ac.getPeriod());
       break;
     case ARRAY:
       verifyArray(label, ec.array(), ac.array());
       break;
     default:
        throw new IllegalStateException( "Unexpected type: " + ec.valueType());
      }
    }
  }

  private void verifyArray(String colLabel, ArrayReader ea,
      ArrayReader aa) {
    assertEquals(colLabel, ea.valueType(), aa.valueType());
    assertEquals(colLabel, ea.size(), aa.size());
    for (int i = 0; i < ea.size(); i++) {
      String label = colLabel + "[" + i + "]";
      switch (ea.valueType()) {
      case ARRAY:
        throw new IllegalStateException("Arrays of arrays not supported yet");
      case BYTES: {
        byte expected[] = ea.getBytes(i);
        byte actual[] = aa.getBytes(i);
        assertEquals(label + " - byte lengths differ", expected.length, actual.length);
        assertTrue(label, Arrays.areEqual(expected, actual));
        break;
      }
      case DOUBLE:
        assertEquals(label, ea.getDouble(i), aa.getDouble(i), delta);
        break;
      case INTEGER:
        assertEquals(label, ea.getInt(i), aa.getInt(i));
        break;
      case LONG:
        assertEquals(label, ea.getLong(i), aa.getLong(i));
        break;
      case STRING:
        assertEquals(label, ea.getString(i), aa.getString(i));
        break;
      case DECIMAL:
        assertEquals(label, ea.getDecimal(i), aa.getDecimal(i));
        break;
      case PERIOD:
        assertEquals(label, ea.getPeriod(i), aa.getPeriod(i));
        break;
      default:
        throw new IllegalStateException( "Unexpected type: " + ea.valueType());
      }
    }
  }
}
