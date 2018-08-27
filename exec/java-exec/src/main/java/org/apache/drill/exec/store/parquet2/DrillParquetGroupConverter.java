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
package org.apache.drill.exec.store.parquet2;

import static org.apache.drill.exec.store.parquet.ParquetReaderUtility.NanoTimeUtils.getDateTimeValueFromBinary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.primitives.Ints;
import org.apache.drill.shaded.guava.com.google.common.primitives.Longs;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.IntervalHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.VarDecimalHolder;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetColumnMetadata;
import org.apache.drill.exec.vector.complex.impl.ComplexWriterImpl;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.BitWriter;
import org.apache.drill.exec.vector.complex.writer.DateWriter;
import org.apache.drill.exec.vector.complex.writer.Float4Writer;
import org.apache.drill.exec.vector.complex.writer.Float8Writer;
import org.apache.drill.exec.vector.complex.writer.IntWriter;
import org.apache.drill.exec.vector.complex.writer.IntervalWriter;
import org.apache.drill.exec.vector.complex.writer.TimeStampWriter;
import org.apache.drill.exec.vector.complex.writer.TimeWriter;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.apache.drill.exec.vector.complex.writer.VarDecimalWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.joda.time.DateTimeConstants;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import io.netty.buffer.DrillBuf;

public class DrillParquetGroupConverter extends GroupConverter {

  private List<Converter> converters;
  private MapWriter mapWriter;
  private final OutputMutator mutator;
  private final OptionManager options;
  // See DRILL-4203
  private final ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates;

  public DrillParquetGroupConverter(OutputMutator mutator, ComplexWriterImpl complexWriter, MessageType schema,
                                    Collection<SchemaPath> columns, OptionManager options,
                                    ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates) {
    this(mutator, complexWriter.rootAsMap(), schema, columns, options, containsCorruptedDates);
  }

  // This function assumes that the fields in the schema parameter are in the same order as the fields in the columns parameter. The
  // columns parameter may have fields that are not present in the schema, though.
  public DrillParquetGroupConverter(OutputMutator mutator, MapWriter mapWriter, GroupType schema,
                                    Collection<SchemaPath> columns, OptionManager options,
                                    ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates) {
    this.mapWriter = mapWriter;
    this.mutator = mutator;
    this.containsCorruptedDates = containsCorruptedDates;
    converters = Lists.newArrayList();
    this.options = options;

    Iterator<SchemaPath> colIterator=columns.iterator();

    for (Type type : schema.getFields()) {
      Repetition rep = type.getRepetition();
      boolean isPrimitive = type.isPrimitive();

      // Match the name of the field in the schema definition to the name of the field in the query.
      String name = null;
      SchemaPath col;
      PathSegment colPath;
      PathSegment colNextChild = null;
      while (colIterator.hasNext()) {
        col = colIterator.next();
        colPath = col.getRootSegment();
        colNextChild = colPath.getChild();

        if (colPath != null && colPath.isNamed() && (!SchemaPath.DYNAMIC_STAR.equals(colPath.getNameSegment().getPath()))) {
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
        Collection<SchemaPath> c = new ArrayList<>();

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
          DrillParquetGroupConverter converter = new DrillParquetGroupConverter(
              mutator, mapWriter.map(name), type.asGroupType(), c, options, containsCorruptedDates);
          converters.add(converter);
        } else {
          DrillParquetGroupConverter converter = new DrillParquetGroupConverter(
              mutator, mapWriter.list(name).map(), type.asGroupType(), c, options, containsCorruptedDates);
          converters.add(converter);
        }
      } else {
        PrimitiveConverter converter = getConverterForType(name, type.asPrimitiveType());
        converters.add(converter);
      }
    }
  }

  @SuppressWarnings("resource")
  private PrimitiveConverter getConverterForType(String name, PrimitiveType type) {

    switch(type.getPrimitiveTypeName()) {
      case INT32: {
        if (type.getOriginalType() == null) {
          IntWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).integer() : mapWriter.integer(name);
          return new DrillIntConverter(writer);
        }
        switch(type.getOriginalType()) {
          case UINT_8 :
          case UINT_16:
          case UINT_32:
          case INT_8  :
          case INT_16 :
          case INT_32 : {
            IntWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).integer() : mapWriter.integer(name);
            return new DrillIntConverter(writer);
          }
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            VarDecimalWriter writer = type.getRepetition() == Repetition.REPEATED
                ? mapWriter.list(name).varDecimal(type.getDecimalMetadata().getScale(), type.getDecimalMetadata().getPrecision())
                : mapWriter.varDecimal(name, type.getDecimalMetadata().getScale(), type.getDecimalMetadata().getPrecision());
            return new DrillVarDecimalConverter(writer, type.getDecimalMetadata().getPrecision(),
                type.getDecimalMetadata().getScale(), mutator.getManagedBuffer());
          }
          case DATE: {
            DateWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).date() : mapWriter.date(name);
            switch(containsCorruptedDates) {
              case META_SHOWS_CORRUPTION:
                return new DrillCorruptedDateConverter(writer);
              case META_SHOWS_NO_CORRUPTION:
                return new DrillDateConverter(writer);
              case META_UNCLEAR_TEST_VALUES:
                return new CorruptionDetectingDateConverter(writer);
              default:
                throw new DrillRuntimeException(
                    String.format("Issue setting up parquet reader for date type, " +
                            "unrecognized date corruption status %s. See DRILL-4203 for more info.",
                        containsCorruptedDates));
            }
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
          // DRILL-6670: handle TIMESTAMP_MICROS as INT64 with no logical type
          case UINT_64:
          case INT_64 :
          case TIMESTAMP_MICROS: {
            BigIntWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).bigInt() : mapWriter.bigInt(name);
            return new DrillBigIntConverter(writer);
          }
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            VarDecimalWriter writer = type.getRepetition() == Repetition.REPEATED
                ? mapWriter.list(name).varDecimal(type.getDecimalMetadata().getScale(), type.getDecimalMetadata().getPrecision())
                : mapWriter.varDecimal(name, type.getDecimalMetadata().getScale(), type.getDecimalMetadata().getPrecision());
            return new DrillVarDecimalConverter(writer, type.getDecimalMetadata().getPrecision(),
                type.getDecimalMetadata().getScale(), mutator.getManagedBuffer());
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
        // TODO: replace null with TIMESTAMP_NANOS once parquet support such type annotation.
        if (type.getOriginalType() == null) {
          if (options.getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP).bool_val) {
            TimeStampWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).timeStamp() : mapWriter.timeStamp(name);
            return new DrillFixedBinaryToTimeStampConverter(writer);
          } else {
            VarBinaryWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).varBinary() : mapWriter.varBinary(name);
            return new DrillFixedBinaryToVarbinaryConverter(writer, ParquetColumnMetadata.getTypeLengthInBits(type.getPrimitiveTypeName()) / 8, mutator.getManagedBuffer());
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
          return new DrillVarBinaryConverter(writer, mutator.getManagedBuffer());
        }
        switch(type.getOriginalType()) {
          case UTF8: {
            VarCharWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).varChar() : mapWriter.varChar(name);
            return new DrillVarCharConverter(writer, mutator.getManagedBuffer());
          }
          case ENUM: {
            VarCharWriter writer = type.getRepetition() == Repetition.REPEATED ? mapWriter.list(name).varChar() : mapWriter.varChar(name);
            return new DrillVarCharConverter(writer, mutator.getManagedBuffer());
          }
          // See DRILL-4184 and DRILL-4834. Support for this is added using new VarDecimal type.
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            DecimalMetadata metadata = type.getDecimalMetadata();
            VarDecimalWriter writer =
                type.getRepetition() == Repetition.REPEATED
                    ? mapWriter.list(name).varDecimal(metadata.getScale(), metadata.getPrecision())
                    : mapWriter.varDecimal(name, metadata.getScale(), metadata.getPrecision());
            return new DrillVarDecimalConverter(writer, metadata.getPrecision(), metadata.getScale(), mutator.getManagedBuffer());
          }
          default: {
            throw new UnsupportedOperationException("Unsupported type " + type.getOriginalType());
          }
        }
      }
      case FIXED_LEN_BYTE_ARRAY:
        switch (type.getOriginalType()) {
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            DecimalMetadata metadata = type.getDecimalMetadata();
            VarDecimalWriter writer = type.getRepetition() == Repetition.REPEATED
                ? mapWriter.list(name).varDecimal(metadata.getScale(), metadata.getPrecision())
                : mapWriter.varDecimal(name, metadata.getScale(), metadata.getPrecision());
            return new DrillVarDecimalConverter(writer, metadata.getPrecision(), metadata.getScale(), mutator.getManagedBuffer());
          }
          case INTERVAL: {
            IntervalWriter writer = type.getRepetition() == Repetition.REPEATED
                ? mapWriter.list(name).interval()
                : mapWriter.interval(name);
            return new DrillFixedLengthByteArrayToInterval(writer);

          }
          default: {
            VarBinaryWriter writer = type.getRepetition() == Repetition.REPEATED
                ? mapWriter.list(name).varBinary()
                : mapWriter.varBinary(name);
            return new DrillFixedBinaryToVarbinaryConverter(writer, type.getTypeLength(), mutator.getManagedBuffer());
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

  public static class CorruptionDetectingDateConverter extends PrimitiveConverter {
    private DateWriter writer;
    private DateHolder holder = new DateHolder();

    public CorruptionDetectingDateConverter(DateWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      if (value > ParquetReaderUtility.DATE_CORRUPTION_THRESHOLD) {
        holder.value = (value - ParquetReaderUtility.CORRECT_CORRUPT_DATE_SHIFT) * DateTimeConstants.MILLIS_PER_DAY;
      } else {
        holder.value = value * (long) DateTimeConstants.MILLIS_PER_DAY;
      }
      writer.write(holder);
    }
  }

  public static class DrillCorruptedDateConverter extends PrimitiveConverter {
    private DateWriter writer;
    private DateHolder holder = new DateHolder();

    public DrillCorruptedDateConverter(DateWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addInt(int value) {
      holder.value = (value - ParquetReaderUtility.CORRECT_CORRUPT_DATE_SHIFT) * DateTimeConstants.MILLIS_PER_DAY;
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
      holder.value = value * (long) DateTimeConstants.MILLIS_PER_DAY;
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

  public static class DrillVarDecimalConverter extends PrimitiveConverter {
    private VarDecimalWriter writer;
    private VarDecimalHolder holder = new VarDecimalHolder();
    private DrillBuf buf;

    public DrillVarDecimalConverter(VarDecimalWriter writer, int precision, int scale, DrillBuf buf) {
      this.writer = writer;
      holder.scale = scale;
      holder.precision = precision;
      this.buf = buf;
    }

    @Override
    public void addBinary(Binary value) {
      holder.buffer = buf.reallocIfNeeded(value.length());
      holder.buffer.setBytes(0, value.toByteBuffer());
      holder.start = 0;
      holder.end = value.length();
      writer.write(holder);
    }

    @Override
    public void addInt(int value) {
      byte[] bytes = Ints.toByteArray(value);
      holder.buffer = buf.reallocIfNeeded(bytes.length);
      holder.buffer.setBytes(0, bytes);
      holder.start = 0;
      holder.end = bytes.length;
      writer.write(holder);
    }

    @Override
    public void addLong(long value) {
      byte[] bytes = Longs.toByteArray(value);
      holder.buffer = buf.reallocIfNeeded(bytes.length);
      holder.buffer.setBytes(0, bytes);
      holder.start = 0;
      holder.end = bytes.length;
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

    @SuppressWarnings("resource")
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

  /**
   * Parquet currently supports a fixed binary type INT96 for storing hive, impala timestamp
   * with nanoseconds precision.
   */
  public static class DrillFixedBinaryToTimeStampConverter extends PrimitiveConverter {
    private TimeStampWriter writer;
    private TimeStampHolder holder = new TimeStampHolder();

    public DrillFixedBinaryToTimeStampConverter(TimeStampWriter writer) {
      this.writer = writer;
    }

    @Override
    public void addBinary(Binary value) {
      holder.value = getDateTimeValueFromBinary(value, true);
      writer.write(holder);
    }
  }
}
