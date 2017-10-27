/**
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
package org.apache.drill.exec.store.hive;

import java.util.Map;

import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableDecimal18Vector;
import org.apache.drill.exec.vector.NullableDecimal28SparseVector;
import org.apache.drill.exec.vector.NullableDecimal38SparseVector;
import org.apache.drill.exec.vector.NullableDecimal9Vector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.collect.Maps;

import static org.apache.drill.exec.store.hive.HiveUtilities.throwUnsupportedHiveDataTypeError;

public abstract class HiveFieldConverter {

  public abstract void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex);

  private static Map<PrimitiveCategory, Class< ? extends HiveFieldConverter>> primMap = Maps.newHashMap();

  // TODO (DRILL-2470)
  // Byte and short (tinyint and smallint in SQL types) are currently read as integers
  // as these smaller integer types are not fully supported in Drill today.
  // Here the same types are used, as we have to read out of the correct typed converter
  // from the hive side, in the FieldConverter classes below for Byte and Short we convert
  // to integer when writing into Drill's vectors.
  static {
    primMap.put(PrimitiveCategory.BINARY, Binary.class);
    primMap.put(PrimitiveCategory.BOOLEAN, Boolean.class);
    primMap.put(PrimitiveCategory.BYTE, Byte.class);
    primMap.put(PrimitiveCategory.DOUBLE, Double.class);
    primMap.put(PrimitiveCategory.FLOAT, Float.class);
    primMap.put(PrimitiveCategory.INT, Int.class);
    primMap.put(PrimitiveCategory.LONG, Long.class);
    primMap.put(PrimitiveCategory.SHORT, Short.class);
    primMap.put(PrimitiveCategory.STRING, String.class);
    primMap.put(PrimitiveCategory.VARCHAR, VarChar.class);
    primMap.put(PrimitiveCategory.TIMESTAMP, Timestamp.class);
    primMap.put(PrimitiveCategory.DATE, Date.class);
    primMap.put(PrimitiveCategory.CHAR, Char.class);
  }


  public static HiveFieldConverter create(TypeInfo typeInfo, FragmentContext fragmentContext)
      throws IllegalAccessException, InstantiationException {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        final PrimitiveCategory pCat = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
        if (pCat != PrimitiveCategory.DECIMAL) {
          Class<? extends HiveFieldConverter> clazz = primMap.get(pCat);
          if (clazz != null) {
            return clazz.newInstance();
          }
        } else {
          // For decimal, based on precision return appropriate converter.
          DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
          int precision = decimalTypeInfo.precision();
          int scale = decimalTypeInfo.scale();
          if (precision <= 9) {
            return new Decimal9(precision, scale);
          } else if (precision <= 18) {
            return new Decimal18(precision, scale);
          } else if (precision <= 28) {
            return new Decimal28(precision, scale, fragmentContext);
          } else {
            return new Decimal38(precision, scale, fragmentContext);
          }
        }

        throwUnsupportedHiveDataTypeError(pCat.toString());
        break;

      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      default:
        throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
    }

    return null;
  }

  public static class Binary extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final byte[] value = ((BinaryObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      ((NullableVarBinaryVector) outputVV).getMutator().setSafe(outputIndex, value, 0, value.length);
    }
  }

  public static class Boolean extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final boolean value = (boolean) ((BooleanObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      ((NullableBitVector) outputVV).getMutator().setSafe(outputIndex, value ? 1 : 0);
    }
  }

  public static class Decimal9 extends HiveFieldConverter {
    private final Decimal9Holder holder = new Decimal9Holder();

    public Decimal9(int precision, int scale) {
      holder.scale = scale;
      holder.precision = precision;
    }

    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      holder.value = DecimalUtility.getDecimal9FromBigDecimal(
          ((HiveDecimalObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue).bigDecimalValue(),
          holder.scale, holder.precision);
      ((NullableDecimal9Vector) outputVV).getMutator().setSafe(outputIndex, holder);
    }
  }

  public static class Decimal18 extends HiveFieldConverter {
    private final Decimal18Holder holder = new Decimal18Holder();

    public Decimal18(int precision, int scale) {
      holder.scale = scale;
      holder.precision = precision;
    }

    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      holder.value = DecimalUtility.getDecimal18FromBigDecimal(
          ((HiveDecimalObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue).bigDecimalValue(),
          holder.scale, holder.precision);
      ((NullableDecimal18Vector) outputVV).getMutator().setSafe(outputIndex, holder);
    }
  }

  public static class Decimal28 extends HiveFieldConverter {
    private final Decimal28SparseHolder holder = new Decimal28SparseHolder();

    public Decimal28(int precision, int scale, FragmentContext context) {
      holder.scale = scale;
      holder.precision = precision;
      holder.buffer = context.getManagedBuffer(Decimal28SparseHolder.nDecimalDigits * DecimalUtility.INTEGER_SIZE);
      holder.start = 0;
    }

    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      DecimalUtility.getSparseFromBigDecimal(
          ((HiveDecimalObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue).bigDecimalValue(),
          holder.buffer, holder.start, holder.scale, holder.precision, Decimal28SparseHolder.nDecimalDigits);
      ((NullableDecimal28SparseVector) outputVV).getMutator().setSafe(outputIndex, holder);
    }
  }

  public static class Decimal38 extends HiveFieldConverter {
    private final Decimal38SparseHolder holder = new Decimal38SparseHolder();

    public Decimal38(int precision, int scale, FragmentContext context) {
      holder.scale = scale;
      holder.precision = precision;
      holder.buffer = context.getManagedBuffer(Decimal38SparseHolder.nDecimalDigits * DecimalUtility.INTEGER_SIZE);
      holder.start = 0;
    }

    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      DecimalUtility.getSparseFromBigDecimal(
          ((HiveDecimalObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue).bigDecimalValue(),
          holder.buffer, holder.start, holder.scale, holder.precision, Decimal38SparseHolder.nDecimalDigits);
      ((NullableDecimal38SparseVector) outputVV).getMutator().setSafe(outputIndex, holder);
    }
  }

  public static class Double extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final double value = (double) ((DoubleObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      ((NullableFloat8Vector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class Float extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final float value = (float) ((FloatObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      ((NullableFloat4Vector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class Int extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final int value = (int) ((IntObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      ((NullableIntVector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  // TODO (DRILL-2470)
  // Byte and short (tinyint and smallint in SQL types) are currently read as integers
  // as these smaller integer types are not fully supported in Drill today.
  public static class Short extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final int value = (short) ((ShortObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      ((NullableIntVector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class Byte extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final int value = (byte)((ByteObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      ((NullableIntVector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class Long extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final long value = (long) ((LongObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      ((NullableBigIntVector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class String extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final Text value = ((StringObjectInspector)oi).getPrimitiveWritableObject(hiveFieldValue);
      final byte[] valueBytes = value.getBytes();
      final int len = value.getLength();
      ((NullableVarCharVector) outputVV).getMutator().setSafe(outputIndex, valueBytes, 0, len);
    }
  }

  public static class VarChar extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final Text value = ((HiveVarcharObjectInspector)oi).getPrimitiveWritableObject(hiveFieldValue).getTextValue();
      final byte[] valueBytes = value.getBytes();
      final int valueLen = value.getLength();
      ((NullableVarCharVector) outputVV).getMutator().setSafe(outputIndex, valueBytes, 0, valueLen);
    }
  }

  public static class Timestamp extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final java.sql.Timestamp value = ((TimestampObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      final DateTime ts = new DateTime(value.getTime()).withZoneRetainFields(DateTimeZone.UTC);
      ((NullableTimeStampVector) outputVV).getMutator().setSafe(outputIndex, ts.getMillis());
    }
  }

  public static class Date extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final java.sql.Date value = ((DateObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      final DateTime date = new DateTime(value.getTime()).withZoneRetainFields(DateTimeZone.UTC);
      ((NullableDateVector) outputVV).getMutator().setSafe(outputIndex, date.getMillis());
    }
  }

  public static class Char extends HiveFieldConverter {
    @Override
    public void setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final Text value = ((HiveCharObjectInspector)oi).getPrimitiveWritableObject(hiveFieldValue).getStrippedValue();
      final byte[] valueBytes = value.getBytes();
      final int valueLen = value.getLength();
      ((NullableVarCharVector) outputVV).getMutator().setSafe(outputIndex, valueBytes, 0, valueLen);
    }
  }

}