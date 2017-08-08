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
package org.apache.drill.exec.physical.rowSet;

import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.ColumnLoader;
import org.apache.drill.exec.physical.rowSet.ScalarLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.vector.accessor.ColumnAccessor.ValueType;
import org.joda.time.Duration;
import org.joda.time.Period;

import com.google.common.base.Charsets;

/**
 * Data converter, to be used with a tuple loader, that converts an int
 * value into each of Drill's data types. The conversions have no real meaning,
 * they are just a way to generate a known, repeatable set of values for
 * each type from a single data converter. The underlying tuple writers
 * handle required/nullable differences and map Drill's many data types to
 * a much smaller set of Java data types. This converter maps those again
 * to ints.
 */

public class TestDataConverter {

  public static abstract class TestColumnConversion {
    protected final ScalarLoader loader;

    public TestColumnConversion(ScalarLoader loader) {
      this.loader = loader;
    }

    public abstract void setInt(int value);
    public abstract Object toObject(int value);
  }

  public static class MissingIntConversion extends TestColumnConversion {

    public MissingIntConversion(ScalarLoader loader) {
      super(loader);
    }

    @Override
    public void setInt(int value) { }

    @Override
    public Object toObject(int value) { return null; }
  }

  public static class IntToIntConversion extends TestColumnConversion {

    public IntToIntConversion(ScalarLoader loader) {
      super(loader);
    }

    @Override
    public void setInt(int value) {
      loader.setInt(value);
    }

    @Override
    public Object toObject(int value) { return value; }
  }

  public static class IntToLongConversion extends TestColumnConversion {

    public IntToLongConversion(ScalarLoader loader) {
      super(loader);
    }

    @Override
    public void setInt(int value) {
      loader.setLong(value);
    }

    @Override
    public Object toObject(int value) { return (long) value; }
  }

  public static class IntToDoubleConversion extends TestColumnConversion {

    public IntToDoubleConversion(ScalarLoader loader) {
      super(loader);
    }

    @Override
    public void setInt(int value) {
      loader.setDouble(value);
    }

    @Override
    public Object toObject(int value) { return (double) value; }
  }

  public static class IntToDecimalConversion extends TestColumnConversion {

    public IntToDecimalConversion(ScalarLoader loader) {
      super(loader);
    }

    @Override
    public void setInt(int value) {
      loader.setDecimal(BigDecimal.valueOf(value));
    }

    @Override
    public Object toObject(int value) { return BigDecimal.valueOf(value); }
  }

  public static class IntToBytesConversion extends TestColumnConversion {

    private byte buf[] = new byte[512];

    public IntToBytesConversion(ScalarLoader loader) {
      super(loader);
    }

    @Override
    public void setInt(int value) {
      loader.setBytes(bytesValue(value));
    }

    private byte[] bytesValue(int value) {
      Arrays.fill(buf, (byte)(value % 256));
      return buf;
    }

    @Override
    public Object toObject(int value) { return bytesValue(value); }

  }

  public static class IntToStringConversion extends TestColumnConversion {

    private byte buf[] = new byte[512];

    public IntToStringConversion(ScalarLoader loader) {
      super(loader);
    }

    @Override
    public void setInt(int value) {
      loader.setString(stringValue(value));
    }

    private String stringValue(int value) {
      Arrays.fill(buf, (byte)(value % 64 + ' '));
      return new String(buf, Charsets.UTF_8);
    }

    @Override
    public Object toObject(int value) { return stringValue(value); }

  }

  public static abstract class IntToPeriodConversion extends TestColumnConversion {

    public IntToPeriodConversion(ScalarLoader loader) {
      super(loader);
    }

    @Override
    public void setInt(int value) {
      loader.setPeriod(periodValue(value));
    }

    @Override
    public Object toObject(int value) { return periodValue(value); }

    protected abstract Period periodValue(int value);
  }

  public static class IntToIntervalDayConversion extends IntToPeriodConversion {

    public IntToIntervalDayConversion(ScalarLoader loader) {
      super(loader);
    }

    @Override
    protected Period periodValue(int value) {
      return Duration.millis(value * 100).toPeriod();
    }
  }

  public static class IntToIntervalYearConversion extends IntToPeriodConversion {

    public IntToIntervalYearConversion(ScalarLoader loader) {
      super(loader);
    }

    @Override
    protected Period periodValue(int value) {
      return Period.months(value / 100);
    }
  }

  public static class IntToIntervalConversion extends IntToPeriodConversion {

    public IntToIntervalConversion(ScalarLoader loader) {
      super(loader);
    }

    // Spread out value into months, days and ms fields.

    @Override
    protected Period periodValue(int value) {
      int months = value / 1000;
      int days = (value % 1000) / 10;
      int ms = value % 10_000 * 100;
      return new Period()
          .plusMonths(months)
          .plusDays(days)
          .plusMillis(ms);
    }
  }

  public TestColumnConversion columns[];

  public TestDataConverter(TupleLoader writer) {
    TupleSchema schema = writer.schema();
    columns = new TestColumnConversion[schema.columnCount()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = createConverter(writer, i);
    }
  }

  private TestColumnConversion createConverter(TupleLoader writer, int colIndex) {
    ColumnLoader loader = writer.column(colIndex);
    if (loader == null) {
      return new MissingIntConversion(loader);
    }
    MinorType type = writer.schema().column(colIndex).getType().getMinorType();
    if (loader.valueType() == ValueType.ARRAY) {
      return createConverter(loader.array(), type);
    } else {
      return createConverter(loader, type);
    }
  }

  private TestColumnConversion createConverter(ScalarLoader loader, MinorType type) {
    switch (loader.valueType()) {
    case BYTES:
      return new IntToBytesConversion(loader);
    case DECIMAL:
      return new IntToDecimalConversion(loader);
    case DOUBLE:
      return new IntToDoubleConversion(loader);
    case INTEGER:
      return new IntToIntConversion(loader);
    case LONG:
      return new IntToLongConversion(loader);
    case PERIOD:
      if (type == MinorType.INTERVALDAY) {
        return new IntToIntervalDayConversion(loader);
      } else if (type == MinorType.INTERVALYEAR) {
        return new IntToIntervalYearConversion(loader);
      } else {
        return new IntToIntervalConversion(loader);
      }
    case STRING:
      return new IntToStringConversion(loader);
    default:
      throw new IllegalStateException("Column type not supported: " + loader.valueType());
    }
  }

  public TestColumnConversion column(int colIndex) { return columns[colIndex]; }
}
