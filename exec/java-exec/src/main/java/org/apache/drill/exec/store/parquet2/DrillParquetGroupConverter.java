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

import io.netty.buffer.DrillBuf;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
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
import org.apache.drill.exec.expr.holders.IntervalHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.util.DecimalUtility;
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
import org.apache.drill.exec.vector.complex.writer.IntervalWriter;
import org.apache.drill.exec.vector.complex.writer.TimeStampWriter;
import org.apache.drill.exec.vector.complex.writer.TimeWriter;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.joda.time.DateTimeUtils;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.google.common.collect.Lists;

public class DrillParquetGroupConverter extends GroupConverter {

  private List<Converter> converters;
  private MapWriter mapWriter;
  private final OutputMutator mutator;
  private final OptionManager options;

  public DrillParquetGroupConverter(OutputMutator mutator, ComplexWriterImpl complexWriter, MessageType schema, Collection<SchemaPath> columns, OptionManager options) {
    this(mutator, complexWriter.rootAsMap(), schema, columns, options);
  }

  // This function assumes that the fields in the schema parameter are in the same order as the fields in the columns parameter. The
  // columns parameter may have fields that are not present in the schema, though.
  public DrillParquetGroupConverter(OutputMutator mutator, MapWriter mapWriter, GroupType schema, Collection<SchemaPath> columns, OptionManager options) {
    this.mapWriter = mapWriter;
    this.mutator = mutator;
    converters = Lists.newArrayList();
    this.options = options;

    Iterator<SchemaPath> colIterator=columns.iterator();

    for (Type type : schema.getFields()) {
      Repetition rep = type.getRepetition();
      boolean isPrimitive = type.isPrimitive();

      // Match the name of the field in the schema definition to the name of the field in the query.
      String name = null;
      SchemaPath col=null;
      PathSegment colPath = null;
      PathSegment colNextChild = null;
      while(colIterator.hasNext()) {
        col = colIterator.next();
        colPath = col.getRootSegment();
        colNextChild = colPath.getChild();

        if (colPath != null && colPath.isNamed() && (!colPath.getNameSegment().getPath().equals("*"))) {
          name = colPath.getNameSegment().getPath();
          // We may have a field that does not exist in the schema
          if (!name.equalsIgnoreCase(type.getName())) {
            continue;
          }
        }
        break;
      }
      if (name == null) {
        name = type.getName();
      }

      if (!isPrimitive) {
        Collection<SchemaPath> c = new ArrayList<SchemaPath>();

        while(colNextChild!=null) {
          if(colNextChild.isNamed()) {
            break;
          }
          colNextChild=colNextChild.getChild();
        }

        if(colNextChild!=null) {
          SchemaPath s = new SchemaPath(colNextChild.getNameSegment());
          c.add(s);
        }
        if (rep != Repetition.REPEATED) {
          DrillParquetGroupConverter converter = new DrillParquetGroupConverter(mutator, mapWriter.map(name), type.asGroupType(), c, options);
          converters.add(converter);
        } else {
          DrillParquetGroupConverter converter = new DrillParquetGroupConverter(mutator, mapWriter.list(name).map(), type.asGroupType(), c, options);
          converters.add(converter);
        }
      } else {
        PrimitiveConverter converter = getConverterForType(name, type.asPrimitiveType());
        converters.add(converter);
      }
    }
  }

  private PrimitiveConverter getConverterForType(String name, PrimitiveType type) {

    switch(type.getPrimitiveTypeName()) {
      case INT32: {
        if (type.getOriginalType() == null) {
          IntWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).integer() : mapWriter.integer(name);
          return new DrillIntConverter(writer);
        }
        switch(type.getOriginalType()) {
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
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
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
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
      case INT96: {
        if (type.getOriginalType() == null) {
          VarBinaryWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).varBinary() : mapWriter.varBinary(name);
          return new DrillFixedBinaryToVarbinaryConverter(writer, ParquetRecordReader.getTypeLengthInBits(type.getPrimitiveTypeName()) / 8, mutator.getManagedBuffer());
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
          return new DrillVarBinaryConverter(writer, mutator.getManagedBuffer());
        }
        switch(type.getOriginalType()) {
          case UTF8: {
            VarCharWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).varChar() : mapWriter.varChar(name);
            return new DrillVarCharConverter(writer, mutator.getManagedBuffer());
          }
          //TODO not sure if BINARY/DECIMAL is actually supported
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            DecimalMetadata metadata = type.getDecimalMetadata();
            if (metadata.getPrecision() <= 28) {
              Decimal28SparseWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).decimal28Sparse() : mapWriter.decimal28Sparse(name, metadata.getScale(), metadata.getPrecision());
              return new DrillBinaryToDecimal28Converter(writer, metadata.getPrecision(), metadata.getScale(), mutator.getManagedBuffer());
            } else {
              Decimal38SparseWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).decimal38Sparse() : mapWriter.decimal38Sparse(name, metadata.getScale(), metadata.getPrecision());
              return new DrillBinaryToDecimal38Converter(writer, metadata.getPrecision(), metadata.getScale(), mutator.getManagedBuffer());
            }
          }
          default: {
            throw new UnsupportedOperationException("Unsupported type " + type.getOriginalType());
          }
        }
      }
      case FIXED_LEN_BYTE_ARRAY:
        if (type.getOriginalType() == OriginalType.DECIMAL) {
          ParquetReaderUtility.checkDecimalTypeEnabled(options);
          DecimalMetadata metadata = type.getDecimalMetadata();
          if (metadata.getPrecision() <= 28) {
            Decimal28SparseWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).decimal28Sparse() : mapWriter.decimal28Sparse(name, metadata.getScale(), metadata.getPrecision());
            return new DrillBinaryToDecimal28Converter(writer, metadata.getPrecision(), metadata.getScale(), mutator.getManagedBuffer());
          } else {
            Decimal38SparseWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).decimal38Sparse() : mapWriter.decimal38Sparse(name, metadata.getScale(), metadata.getPrecision());
            return new DrillBinaryToDecimal38Converter(writer, metadata.getPrecision(), metadata.getScale(), mutator.getManagedBuffer());
          }
        } else if (type.getOriginalType() == OriginalType.INTERVAL) {
          IntervalWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).interval()
              : mapWriter.interval(name);
          return new DrillFixedLengthByteArrayToInterval(writer);

        } else {
          VarBinaryWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).varBinary() : mapWriter.varBinary(name);
          return new DrillFixedBinaryToVarbinaryConverter(writer, type.getTypeLength(), mutator.getManagedBuffer());
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
    public void addLong(long value) {
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
    private DrillBuf buf;
    private VarBinaryHolder holder = new VarBinaryHolder();

    public DrillVarBinaryConverter(VarBinaryWriter writer, DrillBuf buf) {
      this.writer = writer;
      this.buf = buf;
    }

    @Override
    public void addBinary(Binary value) {
      holder.buffer = buf = buf.reallocIfNeeded(value.length());
      buf.setBytes(0, value.toByteBuffer());
      holder.start = 0;
      holder.end = value.length();
      writer.write(holder);
    }
  }

  public static class DrillVarCharConverter extends PrimitiveConverter {
    private VarCharWriter writer;
    private VarCharHolder holder = new VarCharHolder();
    private DrillBuf buf;

    public DrillVarCharConverter(VarCharWriter writer,  DrillBuf buf) {
      this.writer = writer;
      this.buf = buf;
    }

    @Override
    public void addBinary(Binary value) {
      holder.buffer = buf = buf.reallocIfNeeded(value.length());
      buf.setBytes(0, value.toByteBuffer());
      holder.start = 0;
      holder.end = value.length();
      writer.write(holder);
    }
  }

  public static class DrillBinaryToDecimal28Converter extends PrimitiveConverter {
    private Decimal28SparseWriter writer;
    private Decimal28SparseHolder holder = new Decimal28SparseHolder();
    private DrillBuf buf;

    public DrillBinaryToDecimal28Converter(Decimal28SparseWriter writer, int precision, int scale,  DrillBuf buf) {
      this.writer = writer;
      this.buf = buf.reallocIfNeeded(28);
      holder.precision = precision;
      holder.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
      BigDecimal bigDecimal = DecimalUtility.getBigDecimalFromByteArray(value.getBytes(), 0, value.length(), holder.scale);
      DecimalUtility.getSparseFromBigDecimal(bigDecimal, buf, 0, holder.scale, holder.precision, Decimal28SparseHolder.nDecimalDigits);
      holder.buffer = buf;
      writer.write(holder);
    }
  }

  public static class DrillBinaryToDecimal38Converter extends PrimitiveConverter {
    private Decimal38SparseWriter writer;
    private Decimal38SparseHolder holder = new Decimal38SparseHolder();
    private DrillBuf buf;

    public DrillBinaryToDecimal38Converter(Decimal38SparseWriter writer, int precision, int scale,  DrillBuf buf) {
      this.writer = writer;
      this.buf = buf.reallocIfNeeded(38);
      holder.precision = precision;
      holder.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
       BigDecimal bigDecimal = DecimalUtility.getBigDecimalFromByteArray(value.getBytes(), 0, value.length(), holder.scale);
      DecimalUtility.getSparseFromBigDecimal(bigDecimal, buf, 0, holder.scale, holder.precision, Decimal38SparseHolder.nDecimalDigits);
      holder.buffer = buf;
      writer.write(holder);
    }
  }

  public static class DrillFixedLengthByteArrayToInterval extends PrimitiveConverter {
    final private IntervalWriter writer;
    final private IntervalHolder holder = new IntervalHolder();

    public DrillFixedLengthByteArrayToInterval(IntervalWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addBinary(Binary value) {
      final byte[] input = value.getBytes();
      holder.months = ParquetReaderUtility.getIntFromLEBytes(input, 0);
      holder.days = ParquetReaderUtility.getIntFromLEBytes(input, 4);
      holder.milliseconds = ParquetReaderUtility.getIntFromLEBytes(input, 8);
      writer.write(holder);
    }
  }
  /**
   * Parquet currently supports a fixed binary type, which is not implemented in Drill. For now this
   * data will be read in a s varbinary and the same length will be recorded for each value.
   */
  public static class DrillFixedBinaryToVarbinaryConverter extends PrimitiveConverter {
    private VarBinaryWriter writer;
    private VarBinaryHolder holder = new VarBinaryHolder();

    public DrillFixedBinaryToVarbinaryConverter(VarBinaryWriter writer, int length, DrillBuf buf) {
      this.writer = writer;
      holder.buffer = buf = buf.reallocIfNeeded(length);
      holder.start = 0;
      holder.end = length;
    }

    @Override
    public void addBinary(Binary value) {
      holder.buffer.setBytes(0, value.toByteBuffer());
      writer.write(holder);
    }
  }
}
