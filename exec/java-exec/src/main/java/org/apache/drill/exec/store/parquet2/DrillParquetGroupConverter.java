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
package org.apache.drill.exec.store.parquet2;

import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.drill.common.util.DecimalUtility;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.drill.exec.store.parquet.columnreaders.NullableFixedByteAlignedReaders;
import org.apache.drill.exec.vector.complex.impl.ComplexWriterImpl;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.BitWriter;
import org.apache.drill.exec.vector.complex.writer.DateWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal18Writer;
import org.apache.drill.exec.vector.complex.writer.Decimal28SparseWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal38SparseWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal9Writer;
import org.apache.drill.exec.vector.complex.writer.Float4Writer;
import org.apache.drill.exec.vector.complex.writer.Float8Writer;
import org.apache.drill.exec.vector.complex.writer.IntWriter;
import org.apache.drill.exec.vector.complex.writer.TimeStampWriter;
import org.apache.drill.exec.vector.complex.writer.TimeWriter;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.joda.time.DateTimeUtils;

import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.DecimalMetadata;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

import java.math.BigDecimal;
import java.util.List;

public class DrillParquetGroupConverter extends GroupConverter {

  private List<Converter> converters;
  private MapWriter mapWriter;

  public DrillParquetGroupConverter(ComplexWriterImpl complexWriter, MessageType schema) {
    this(complexWriter.rootAsMap(), schema);
  }

  public DrillParquetGroupConverter(MapWriter mapWriter, GroupType schema) {
    this.mapWriter = mapWriter;
    converters = Lists.newArrayList();
    for (Type type : schema.getFields()) {
      Repetition rep = type.getRepetition();
      boolean isPrimitive = type.isPrimitive();
      if (!isPrimitive) {
        if (rep != Repetition.REPEATED) {
          DrillParquetGroupConverter converter = new DrillParquetGroupConverter(mapWriter.map(type.getName()), type.asGroupType());
          converters.add(converter);
        } else {
          DrillParquetGroupConverter converter = new DrillParquetGroupConverter(mapWriter.list(type.getName()).map(), type.asGroupType());
          converters.add(converter);
        }
      } else {
        PrimitiveConverter converter = getConverterForType(type.asPrimitiveType());
        converters.add(converter);
      }
    }
  }

  private PrimitiveConverter getConverterForType(PrimitiveType type) {

    String name = type.getName();
    switch(type.getPrimitiveTypeName()) {
      case INT32: {
        if (type.getOriginalType() == null) {
          IntWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).integer() : mapWriter.integer(name);
          return new DrillIntConverter(writer);
        }
        switch(type.getOriginalType()) {
          case DECIMAL: {
            Decimal9Writer writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).decimal9() : mapWriter.decimal9(name);
            return new DrillDecimal9Converter(writer, type.getDecimalMetadata().getPrecision(), type.getDecimalMetadata().getScale());
          }
          case DATE: {
            DateWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).date() : mapWriter.date(name);
            return new DrillDateConverter(writer);
          }
          case TIME_MILLIS: {
            TimeWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).time() : mapWriter.time(name);
            return new DrillTimeConverter(writer);
          }
          default: {
            throw new UnsupportedOperationException("Unsupported type: " + type.getOriginalType());
          }
        }
      }
      case INT64: {
        if (type.getOriginalType() == null) {
          BigIntWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).bigInt() : mapWriter.bigInt(name);
          return new DrillBigIntConverter(writer);
        }
        switch(type.getOriginalType()) {
          case DECIMAL: {
            Decimal18Writer writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).decimal18() : mapWriter.decimal18(name);
            return new DrillDecimal18Converter(writer, type.getDecimalMetadata().getPrecision(), type.getDecimalMetadata().getScale());
          }
          case TIMESTAMP_MILLIS: {
            TimeStampWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).timeStamp() : mapWriter.timeStamp(name);
            return new DrillTimeStampConverter(writer);
          }
          default: {
            throw new UnsupportedOperationException("Unsupported type " + type.getOriginalType());
          }
        }
      }
      case FLOAT: {
        Float4Writer writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).float4() : mapWriter.float4(name);
        return new DrillFloat4Converter(writer);
      }
      case DOUBLE: {
        Float8Writer writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).float8() : mapWriter.float8(name);
        return new DrillFloat8Converter(writer);
      }
      case BOOLEAN: {
        BitWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).bit() : mapWriter.bit(name);
        return new DrillBoolConverter(writer);
      }
      case BINARY: {
        if (type.getOriginalType() == null) {
          VarBinaryWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).varBinary() : mapWriter.varBinary(name);
          return new DrillVarBinaryConverter(writer);
        }
        switch(type.getOriginalType()) {
          case UTF8: {
            VarCharWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).varChar() : mapWriter.varChar(name);
            return new DrillVarCharConverter(writer);
          }
          case DECIMAL: {
            DecimalMetadata metadata = type.getDecimalMetadata();
            if (metadata.getPrecision() <= 28) {
              Decimal28SparseWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).decimal28Sparse() : mapWriter.decimal28Sparse(name);
              return new DrillBinaryToDecimal28Converter(writer, metadata.getPrecision(), metadata.getScale());
            } else {
              Decimal38SparseWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).decimal38Sparse() : mapWriter.decimal38Sparse(name);
              return new DrillBinaryToDecimal38Converter(writer, metadata.getPrecision(), metadata.getScale());
            }
          }
          default: {
            throw new UnsupportedOperationException("Unsupported type " + type.getOriginalType());
          }
        }
      }
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type.getPrimitiveTypeName());
    }
  }

  @Override
  public Converter getConverter(int i) {
    return converters.get(i);
  }

  @Override
  public void start() {
    mapWriter.start();
  }

  @Override
  public void end() {
    mapWriter.end();
  }

  public static class DrillIntConverter extends PrimitiveConverter {
    private IntWriter writer;
    private IntHolder holder = new IntHolder();

    public DrillIntConverter(IntWriter writer) {
      super();
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      holder.value = value;
      writer.write(holder);
    }
  }

  public static class DrillDecimal9Converter extends PrimitiveConverter {
    private Decimal9Writer writer;
    private Decimal9Holder holder = new Decimal9Holder();
    int precision;
    int scale;

    public DrillDecimal9Converter(Decimal9Writer writer, int precision, int scale) {
      this.writer = writer;
      this.scale = scale;
      this.precision = precision;
    }

    @Override
    public void addInt(int value) {
      holder.value = value;
      writer.write(holder);
    }
  }

  public static class DrillDateConverter extends PrimitiveConverter {
    private DateWriter writer;
    private DateHolder holder = new DateHolder();

    public DrillDateConverter(DateWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      holder.value = DateTimeUtils.fromJulianDay(value - ParquetOutputRecordWriter.JULIAN_DAY_EPOC - 0.5);
      writer.write(holder);
    }
  }

  public static class DrillTimeConverter extends PrimitiveConverter {
    private TimeWriter writer;
    private TimeHolder holder = new TimeHolder();

    public DrillTimeConverter(TimeWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      holder.value = value;
      writer.write(holder);
    }
  }

  public static class DrillBigIntConverter extends PrimitiveConverter {
    private BigIntWriter writer;
    private BigIntHolder holder = new BigIntHolder();

    public DrillBigIntConverter(BigIntWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addLong(long value) {
      holder.value = value;
      writer.write(holder);
    }
  }

  public static class DrillTimeStampConverter extends PrimitiveConverter {
    private TimeStampWriter writer;
    private TimeStampHolder holder = new TimeStampHolder();

    public DrillTimeStampConverter(TimeStampWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      holder.value = value;
      writer.write(holder);
    }
  }

  public static class DrillDecimal18Converter extends PrimitiveConverter {
    private Decimal18Writer writer;
    private Decimal18Holder holder = new Decimal18Holder();

    public DrillDecimal18Converter(Decimal18Writer writer, int precision, int scale) {
      this.writer = writer;
      holder.precision = precision;
      holder.scale = scale;
    }

    @Override
    public void addLong(long value) {
      holder.value = value;
      writer.write(holder);
    }
  }

  public static class DrillFloat4Converter extends PrimitiveConverter {
    private Float4Writer writer;
    private Float4Holder holder = new Float4Holder();

    public DrillFloat4Converter(Float4Writer writer) {
      this.writer = writer;
    }

    @Override
    public void addFloat(float value) {
      holder.value = value;
      writer.write(holder);
    }
  }

  public static class DrillFloat8Converter extends PrimitiveConverter {
    private Float8Writer writer;
    private Float8Holder holder = new Float8Holder();

    public DrillFloat8Converter(Float8Writer writer) {
      this.writer = writer;
    }

    @Override
    public void addDouble(double value) {
      holder.value = value;
      writer.write(holder);
    }
  }

  public static class DrillBoolConverter extends PrimitiveConverter {
    private BitWriter writer;
    private BitHolder holder = new BitHolder();

    public DrillBoolConverter(BitWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addBoolean(boolean value) {
      holder.value = value ? 1 : 0;
      writer.write(holder);
    }
  }

  public static class DrillVarBinaryConverter extends PrimitiveConverter {
    private VarBinaryWriter writer;
    private VarBinaryHolder holder = new VarBinaryHolder();

    public DrillVarBinaryConverter(VarBinaryWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addBinary(Binary value) {
      ByteBuf buf = Unpooled.wrappedBuffer(value.toByteBuffer());
      holder.buffer = buf;
      holder.start = 0;
      holder.end = value.length();
      writer.write(holder);
    }
  }

  public static class DrillVarCharConverter extends PrimitiveConverter {
    private VarCharWriter writer;
    private VarCharHolder holder = new VarCharHolder();

    public DrillVarCharConverter(VarCharWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addBinary(Binary value) {
      ByteBuf buf = Unpooled.wrappedBuffer(value.toByteBuffer());
      holder.buffer = buf;
      holder.start = 0;
      holder.end = value.length();
      writer.write(holder);
    }
  }

  public static class DrillBinaryToDecimal28Converter extends PrimitiveConverter {
    private Decimal28SparseWriter writer;
    private Decimal28SparseHolder holder = new Decimal28SparseHolder();

    public DrillBinaryToDecimal28Converter(Decimal28SparseWriter writer, int precision, int scale) {
      this.writer = writer;
      holder.precision = precision;
      holder.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
      BigDecimal bigDecimal = DecimalUtility.getBigDecimalFromByteArray(value.getBytes(), 0, value.length(), holder.scale);
      ByteBuf buf = Unpooled.wrappedBuffer(new byte[28]);
      DecimalUtility.getSparseFromBigDecimal(bigDecimal, buf, 0, holder.scale, holder.precision, Decimal28SparseHolder.nDecimalDigits);
      holder.buffer = buf;
      writer.write(holder);
    }
  }

  public static class DrillBinaryToDecimal38Converter extends PrimitiveConverter {
    private Decimal38SparseWriter writer;
    private Decimal38SparseHolder holder = new Decimal38SparseHolder();

    public DrillBinaryToDecimal38Converter(Decimal38SparseWriter writer, int precision, int scale) {
      this.writer = writer;
      holder.precision = precision;
      holder.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
      BigDecimal bigDecimal = DecimalUtility.getBigDecimalFromByteArray(value.getBytes(), 0, value.length(), holder.scale);
      ByteBuf buf = Unpooled.wrappedBuffer(new byte[38]);
      DecimalUtility.getSparseFromBigDecimal(bigDecimal, buf, 0, holder.scale, holder.precision, Decimal38SparseHolder.nDecimalDigits);
      holder.buffer = buf;
      writer.write(holder);
    }
  }
}
