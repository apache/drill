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
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.bouncycastle.util.Arrays;

import java.util.Comparator;

/**
 * For testing, compare the contents of two row sets (record batches)
 * to verify that they are identical. Supports masks to exclude certain
 * columns from comparison.
 * <p>
 * Drill rows are analogous to JSON documents: they can have scalars,
 * arrays and maps, with maps and lists holding maps, arrays and scalars.
 * This class walks the row structure tree to compare each structure
 * of two row sets checking counts, types and values to ensure that the
 * "actual" result set (result of a test) matches the "expected" result
 * set.
 * <p>
 * This class acts as an example of how to use the suite of reader
 * abstractions.
 */

public class RowSetComparison {

  /**
   * Row set with the expected outcome of a test. This is the "golden"
   * copy defined in the test itself.
   */
  private RowSet expected;
  /**
   * Some tests wish to ignore certain (top-level) columns. If a
   * mask is provided, then only those columns with a <tt>true</tt>
   * will be verified.
   */
  private boolean mask[];
  /**
   * Floats and doubles do not compare exactly. This delta is used
   * by JUnit for such comparisons.
   */
  private double delta = 0.001;
  /**
   * Tests can skip the first n rows.
   */
  private int offset;
  private int span = -1;

  public RowSetComparison(RowSet expected) {
    this.expected = expected;

    // TODO: The mask only works at the top level presently

    mask = new boolean[expected.schema().size()];
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
      String label = Integer.toString(er.index() + 1);
      verifyRow(label, er, ar);
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

  private void verifyRow(String label, TupleReader er, TupleReader ar) {
    String prefix = label + ":";
    for (int i = 0; i < mask.length; i++) {
      if (! mask[i]) {
        continue;
      }
      verifyColumn(prefix + i, er.column(i), ar.column(i));
    }
  }

  private void verifyColumn(String label, ObjectReader ec, ObjectReader ac) {
    assertEquals(label, ec.type(), ac.type());
    switch (ec.type()) {
    case ARRAY:
      verifyArray(label, ec.array(), ac.array());
      break;
    case SCALAR:
      verifyScalar(label, ec.scalar(), ac.scalar());
      break;
    case TUPLE:
      verifyTuple(label, ec.tuple(), ac.tuple());
      break;
    default:
      throw new IllegalStateException( "Unexpected type: " + ec.type());
    }
  }

  private void verifyTuple(String label, TupleReader er, TupleReader ar) {
    assertEquals(label + " - tuple count", er.columnCount(), ar.columnCount());
    String prefix = label + ":";
    for (int i = 0; i < er.columnCount(); i++) {
      verifyColumn(prefix + i, er.column(i), ar.column(i));
    }
  }

  private void verifyScalar(String label, ScalarReader ec, ScalarReader ac) {
    assertEquals(label + " - value type", ec.valueType(), ac.valueType());
    if (ec.isNull()) {
      assertTrue(label + " - column not null", ac.isNull());
      return;
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
     default:
        throw new IllegalStateException( "Unexpected type: " + ec.valueType());
    }
  }

  private void verifyArray(String label, ArrayReader ea,
      ArrayReader aa) {
    assertEquals(label, ea.entryType(), aa.entryType());
    assertEquals(label, ea.size(), aa.size());
    switch (ea.entryType()) {
    case ARRAY:
      throw new UnsupportedOperationException();
    case SCALAR:
      verifyScalarArray(label, ea.elements(), aa.elements());
      break;
    case TUPLE:
      verifyTupleArray(label, ea, aa);
      break;
    default:
      throw new IllegalStateException( "Unexpected type: " + ea.entryType());
    }
  }

  private void verifyTupleArray(String label, ArrayReader ea, ArrayReader aa) {
    for (int i = 0; i < ea.size(); i++) {
      verifyTuple(label + "[" + i + "]", ea.tuple(i), aa.tuple(i));
    }
  }

  private void verifyScalarArray(String colLabel, ScalarElementReader ea,
      ScalarElementReader aa) {
    assertEquals(colLabel, ea.valueType(), aa.valueType());
    assertEquals(colLabel, ea.size(), aa.size());
    for (int i = 0; i < ea.size(); i++) {
      String label = colLabel + "[" + i + "]";
      switch (ea.valueType()) {
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

  // TODO make a native RowSetComparison comparator
  public static class ObjectComparator implements Comparator<Object> {
    public static final ObjectComparator INSTANCE = new ObjectComparator();

    private ObjectComparator() {
    }

    @Override
    public int compare(Object a, Object b) {
      if (a instanceof Integer) {
        int aInt = (Integer) a;
        int bInt = (Integer) b;
        return aInt - bInt;
      } else if (a instanceof Long) {
        Long aLong = (Long) a;
        Long bLong = (Long) b;
        return aLong.compareTo(bLong);
      } else if (a instanceof Float) {
        Float aFloat = (Float) a;
        Float bFloat = (Float) b;
        return aFloat.compareTo(bFloat);
      } else if (a instanceof Double) {
        Double aDouble = (Double) a;
        Double bDouble = (Double) b;
        return aDouble.compareTo(bDouble);
      } else if (a instanceof String) {
        String aString = (String) a;
        String bString = (String) b;
        return aString.compareTo(bString);
      } else {
        throw new UnsupportedOperationException(String.format("Unsupported type %s", a.getClass().getCanonicalName()));
      }
    }
  }
}
