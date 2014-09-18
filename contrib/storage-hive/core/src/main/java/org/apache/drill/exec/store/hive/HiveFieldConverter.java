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

import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableSmallIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableTinyIntVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.collect.Maps;

public abstract class HiveFieldConverter {

  public abstract boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex);

  private static Map<PrimitiveCategory, Class< ? extends HiveFieldConverter>> primMap = Maps.newHashMap();

  static {
    primMap.put(PrimitiveCategory.BINARY, Binary.class);
    primMap.put(PrimitiveCategory.BOOLEAN, Boolean.class);
    primMap.put(PrimitiveCategory.BYTE, Byte.class);
    primMap.put(PrimitiveCategory.DECIMAL, Decimal.class);
    primMap.put(PrimitiveCategory.DOUBLE, Double.class);
    primMap.put(PrimitiveCategory.FLOAT, Float.class);
    primMap.put(PrimitiveCategory.INT, Int.class);
    primMap.put(PrimitiveCategory.LONG, Long.class);
    primMap.put(PrimitiveCategory.SHORT, Short.class);
    primMap.put(PrimitiveCategory.STRING, String.class);
    primMap.put(PrimitiveCategory.VARCHAR, VarChar.class);
    primMap.put(PrimitiveCategory.TIMESTAMP, Timestamp.class);
    primMap.put(PrimitiveCategory.DATE, Date.class);
  }


  public static HiveFieldConverter create(TypeInfo typeInfo) throws IllegalAccessException, InstantiationException {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        final PrimitiveCategory pCat = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
        Class< ? extends HiveFieldConverter> clazz = primMap.get(pCat);
        if (clazz != null) {
          return clazz.newInstance();
        }

        HiveRecordReader.throwUnsupportedHiveDataTypeError(pCat.toString());
        break;

      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      default:
        HiveRecordReader.throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
    }

    return null;
  }

  public static class Binary extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final byte[] value = ((BinaryObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      return ((NullableVarBinaryVector) outputVV).getMutator().setSafe(outputIndex, value, 0, value.length);
    }
  }

  public static class Boolean extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final boolean value = (boolean) ((BooleanObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      return ((NullableBitVector) outputVV).getMutator().setSafe(outputIndex, value ? 1 : 0);
    }
  }

  public static class Byte extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final byte value = (byte) ((ByteObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      return ((NullableTinyIntVector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class Decimal extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final HiveDecimal value = ((HiveDecimalObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      final byte[] strBytes = value.toString().getBytes();
      return ((NullableVarCharVector) outputVV).getMutator().setSafe(outputIndex, strBytes, 0, strBytes.length);
    }
  }

  public static class Double extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final double value = (double) ((DoubleObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      return ((NullableFloat8Vector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class Float extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final float value = (float) ((FloatObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      return ((NullableFloat4Vector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class Int extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final int value = (int) ((IntObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      return ((NullableIntVector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class Long extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final long value = (long) ((LongObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      return ((NullableBigIntVector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class Short extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final short value = (short) ((ShortObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      return ((NullableSmallIntVector) outputVV).getMutator().setSafe(outputIndex, value);
    }
  }

  public static class String extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final Text value = ((StringObjectInspector)oi).getPrimitiveWritableObject(hiveFieldValue);
      final byte[] valueBytes = value.getBytes();
      final int len = value.getLength();
      return ((NullableVarCharVector) outputVV).getMutator().setSafe(outputIndex, valueBytes, 0, len);
    }
  }

  public static class VarChar extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final Text value = ((HiveVarcharObjectInspector)oi).getPrimitiveWritableObject(hiveFieldValue).getTextValue();
      final byte[] valueBytes = value.getBytes();
      final int valueLen = value.getLength();
      return ((NullableVarCharVector) outputVV).getMutator().setSafe(outputIndex, valueBytes, 0, valueLen);
    }
  }

  public static class Timestamp extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final java.sql.Timestamp value = ((TimestampObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      final DateTime ts = new DateTime(value.getTime()).withZoneRetainFields(DateTimeZone.UTC);
      return ((NullableTimeStampVector) outputVV).getMutator().setSafe(outputIndex, ts.getMillis());
    }
  }

  public static class Date extends HiveFieldConverter {
    @Override
    public boolean setSafeValue(ObjectInspector oi, Object hiveFieldValue, ValueVector outputVV, int outputIndex) {
      final java.sql.Date value = ((DateObjectInspector)oi).getPrimitiveJavaObject(hiveFieldValue);
      final DateTime date = new DateTime(value.getTime()).withZoneRetainFields(DateTimeZone.UTC);
      return ((NullableDateVector) outputVV).getMutator().setSafe(outputIndex, date.getMillis());
    }
  }

}
