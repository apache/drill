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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.accessor.AccessorUtilities;
import org.apache.drill.exec.vector.accessor.ColumnAccessor.ValueType;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.joda.time.Duration;
import org.joda.time.Period;

/**
 * Various utilities useful for working with row sets, especially for testing.
 */

public class RowSetUtilities {

  private RowSetUtilities() { }

  /**
   * Reverse a row set by reversing the entries in an SV2. This is a quick
   * and easy way to reverse the sort order of an expected-value row set.
   * @param sv2 the SV2 which is reversed in place
   */

  public static void reverse(SelectionVector2 sv2) {
    int count = sv2.getCount();
    for (int i = 0; i < count / 2; i++) {
      char temp = sv2.getIndex(i);
      int dest = count - 1 - i;
      sv2.setIndex(i, sv2.getIndex(dest));
      sv2.setIndex(dest, temp);
    }
  }

  /**
   * Set a test data value from an int. Uses the type information of the
   * column to handle interval types. Else, uses the value type of the
   * accessor. The value set here is purely for testing; the mapping
   * from ints to intervals has no real meaning.
   *
   * @param rowWriter
   * @param index
   * @param value
   */

  public static void setFromInt(RowSetWriter rowWriter, int index, int value) {
    ColumnWriter writer = rowWriter.column(index);
    if (writer.valueType() == ValueType.PERIOD) {
      setPeriodFromInt(writer, rowWriter.schema().column(index).getType().getMinorType(), value);
    } else {
      AccessorUtilities.setFromInt(writer, value);
    }
  }

  /**
   * Ad-hoc, test-only method to set a Period from an integer. Periods are made up of
   * months and millseconds. There is no mapping from one to the other, so a period
   * requires at least two number. Still, we are given just one (typically from a test
   * data generator.) Use that int value to "spread" some value across the two kinds
   * of fields. The result has no meaning, but has the same comparison order as the
   * original ints.
   *
   * @param writer column writer for a period column
   * @param minorType the Drill data type
   * @param value the integer value to apply
   */

  public static void setPeriodFromInt(ColumnWriter writer, MinorType minorType,
      int value) {
    switch (minorType) {
    case INTERVAL:
      writer.setPeriod(Duration.millis(value).toPeriod());
      break;
    case INTERVALYEAR:
      writer.setPeriod(Period.years(value / 12).withMonths(value % 12));
      break;
    case INTERVALDAY:
      int sec = value % 60;
      value = value / 60;
      int min = value % 60;
      value = value / 60;
      writer.setPeriod(Period.days(value).withMinutes(min).withSeconds(sec));
      break;
    default:
      throw new IllegalArgumentException("Writer is not an interval: " + minorType);
    }
  }
}
