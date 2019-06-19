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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.netty.buffer.DrillBuf;
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
import org.apache.drill.exec.vector.complex.impl.RepeatedMapWriter;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
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
import org.apache.drill.shaded.guava.com.google.common.primitives.Ints;
import org.apache.drill.shaded.guava.com.google.common.primitives.Longs;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.joda.time.DateTimeConstants;

import static org.apache.drill.common.expression.SchemaPath.DYNAMIC_STAR;
import static org.apache.drill.exec.store.parquet.ParquetReaderUtility.NanoTimeUtils.getDateTimeValueFromBinary;

public class DrillParquetGroupConverter extends GroupConverter {

  private final List<Converter> converters;
  private final BaseWriter baseWriter;
  private final OutputMutator mutator;
  private final OptionManager options;
  // See DRILL-4203
  private final ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates;

  /**
   * Debugging information in form of "parent">fieldName[WriterClassName-hashCode()],
   * where "parent" is parent converterName.
   */
  private final String converterName;

  /**
   * The constructor is responsible for creation of converters tree and may invoke itself for
   * creation of child converters when nested field is group type field too. Assumed that ordering of
   * fields from schema parameter matches ordering of paths in columns list. Though columns may have fields
   * which aren't present in schema.
   *
   * @param mutator                output mutator, used to share managed buffer with primitive converters
   * @param baseWriter             map or list writer associated with the group converter
   * @param schema                 group type of the converter
   * @param columns                columns to project
   * @param options                option manager used to check enabled option when necessary
   * @param containsCorruptedDates allows to select strategy for dates handling
   * @param skipRepeated           true only if parent field in schema detected as list and current schema is repeated group type
   * @param parentName             name of group converter which called the constructor
   */
  public DrillParquetGroupConverter(OutputMutator mutator, BaseWriter baseWriter, GroupType schema,
                                    Collection<SchemaPath> columns, OptionManager options,
                                    ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates,
                                    boolean skipRepeated, String parentName) {
    this.converterName = String.format("%s>%s[%s-%d]", parentName, schema.getName(), baseWriter.getClass().getSimpleName(), baseWriter.hashCode());
    this.baseWriter = baseWriter;
    this.mutator = mutator;
    this.containsCorruptedDates = containsCorruptedDates;
    this.converters = new ArrayList<>();
    this.options = options;

    Iterator<SchemaPath> colIterator = columns.iterator();

    for (final Type type : schema.getFields()) {

      // Match the name of the field in the schema definition to the name of the field in the query.
      String name = type.getName();
      PathSegment colNextChild = null;
      while (colIterator.hasNext()) {
        PathSegment colPath = colIterator.next().getRootSegment();
        String colPathName;
        if (colPath.isNamed() &&
            !DYNAMIC_STAR.equals(colPathName = colPath.getNameSegment().getPath()) &&
            colPathName.equalsIgnoreCase(name)) {
          name = colPathName;
          colNextChild = colPath.getChild();
          break;
        }
      }

      Converter converter = createFieldConverter(skipRepeated, type, name, colNextChild);
      converters.add(converter);
    }
  }

  private Converter createFieldConverter(boolean skipRepeated, Type fieldType, String name, PathSegment colNextChild) {
    Converter converter;
    if (fieldType.isPrimitive()) {
      converter = getConverterForType(name, fieldType.asPrimitiveType());
    } else {
      while (colNextChild != null && !colNextChild.isNamed()) {
        colNextChild = colNextChild.getChild();
      }

      Collection<SchemaPath> columns = colNextChild == null
          ? Collections.emptyList()
          : Collections.singletonList(new SchemaPath(colNextChild.getNameSegment()));

      BaseWriter writer;
      GroupType fieldGroupType = fieldType.asGroupType();
      if (isLogicalListType(fieldGroupType)) {
        writer = getWriter(name, (m, s) -> m.list(s), l -> l.list());
        converter = new DrillParquetGroupConverter(mutator, writer, fieldGroupType, columns, options,
            containsCorruptedDates, true, converterName);
      } else if (fieldType.isRepetition(Repetition.REPEATED)) {
        if (skipRepeated) {
          converter = new DrillIntermediateParquetGroupConverter(mutator, baseWriter, fieldGroupType, columns, options,
              containsCorruptedDates, false, converterName);
        } else {
          writer = getWriter(name, (m, s) -> m.list(s).map(), l -> l.list().map());
          converter = new DrillParquetGroupConverter(mutator, writer, fieldGroupType, columns, options,
              containsCorruptedDates, false, converterName);
        }
      } else {
        writer = getWriter(name, (m, s) -> m.map(s), l -> l.map());
        converter = new DrillParquetGroupConverter(mutator, writer, fieldGroupType, columns, options,
            containsCorruptedDates, false, converterName);
      }

    }
    return converter;
  }

  /**
   * Checks whether group field approximately matches pattern for Logical Lists:
   * <list-repetition> group <name> (LIST) {
   *   repeated group list {
   *     <element-repetition> <element-type> element;
   *   }
   * }
   * (See for more details: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists)
   *
   * Note, that standard field names 'list' and 'element' aren't checked intentionally,
   * because Hive lists have 'bag' and 'array_element' names instead.
   *
   * @param groupType type which may have LIST original type
   * @return whether the type is LIST and nested field is repeated group
   */
  private boolean isLogicalListType(GroupType groupType) {
    if (groupType.getOriginalType() == OriginalType.LIST && groupType.getFieldCount() == 1) {
      Type nestedField = groupType.getFields().get(0);
      return nestedField.isRepetition(Repetition.REPEATED)
          && !nestedField.isPrimitive()
          && nestedField.getOriginalType() == null
          && nestedField.asGroupType().getFieldCount() == 1;
    }
    return false;
  }

  private PrimitiveConverter getConverterForType(String name, PrimitiveType type) {
    switch(type.getPrimitiveTypeName()) {
      case INT32: {
        if (type.getOriginalType() == null) {
          return getIntConverter(name, type);
        }
        switch(type.getOriginalType()) {
          case UINT_8 :
          case UINT_16:
          case UINT_32:
          case INT_8  :
          case INT_16 :
          case INT_32 : {
            return getIntConverter(name, type);
          }
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            return getVarDecimalConverter(name, type);
          }
          case DATE: {
            DateWriter writer = type.isRepetition(Repetition.REPEATED)
                ? getWriter(name, (m, f) -> m.list(f).date(), l -> l.list().date())
                : getWriter(name, (m, f) -> m.date(f), l -> l.date());

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
            TimeWriter writer = type.isRepetition(Repetition.REPEATED)
                ? getWriter(name, (m, f) -> m.list(f).time(), l -> l.list().time())
                : getWriter(name, (m, f) -> m.time(f), l -> l.time());
            return new DrillTimeConverter(writer);
          }
          default: {
            throw new UnsupportedOperationException("Unsupported type: " + type.getOriginalType());
          }
        }
      }
      case INT64: {
        if (type.getOriginalType() == null) {
          return getBigIntConverter(name, type);
        }
        switch(type.getOriginalType()) {
          // DRILL-6670: handle TIMESTAMP_MICROS as INT64 with no logical type
          case UINT_64:
          case INT_64 :
          case TIMESTAMP_MICROS: {
            return getBigIntConverter(name, type);
          }
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            return getVarDecimalConverter(name, type);
          }
          case TIMESTAMP_MILLIS: {
            TimeStampWriter writer = getTimeStampWriter(name, type);
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
            TimeStampWriter writer = getTimeStampWriter(name, type);
            return new DrillFixedBinaryToTimeStampConverter(writer);
          } else {
            VarBinaryWriter writer = type.isRepetition(Repetition.REPEATED)
                ? getWriter(name, (m, f) -> m.list(f).varBinary(), l -> l.list().varBinary())
                : getWriter(name, (m, f) -> m.varBinary(f), listWriter -> listWriter.varBinary());
            return new DrillFixedBinaryToVarbinaryConverter(writer, ParquetColumnMetadata.getTypeLengthInBits(type.getPrimitiveTypeName()) / 8, mutator.getManagedBuffer());
          }
        }

      }
      case FLOAT: {
        Float4Writer writer = type.isRepetition(Repetition.REPEATED)
            ? getWriter(name, (m, f) -> m.list(f).float4(), l -> l.list().float4())
            : getWriter(name, (m, f) -> m.float4(f), l -> l.float4());
        return new DrillFloat4Converter(writer);
      }
      case DOUBLE: {
        Float8Writer writer = type.isRepetition(Repetition.REPEATED)
            ? getWriter(name, (m, f) -> m.list(f).float8(), l -> l.list().float8())
            : getWriter(name, (m, f) -> m.float8(f), l -> l.float8());
        return new DrillFloat8Converter(writer);
      }
      case BOOLEAN: {
        BitWriter writer = type.isRepetition(Repetition.REPEATED)
            ? getWriter(name, (m, f) -> m.list(f).bit(), l -> l.list().bit())
            : getWriter(name, (m, f) -> m.bit(f), l -> l.bit());
        return new DrillBoolConverter(writer);
      }
      case BINARY: {
        if (type.getOriginalType() == null) {
          VarBinaryWriter writer = type.isRepetition(Repetition.REPEATED)
              ? getWriter(name, (m, f) -> m.list(f).varBinary(), l -> l.list().varBinary())
              : getWriter(name, (m, f) -> m.varBinary(f), l -> l.varBinary());
          return new DrillVarBinaryConverter(writer, mutator.getManagedBuffer());
        }
        switch(type.getOriginalType()) {
          case UTF8: {
            return getVarCharConverter(name, type);
          }
          case ENUM: {
            return getVarCharConverter(name, type);
          }
          // See DRILL-4184 and DRILL-4834. Support for this is added using new VarDecimal type.
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            return getVarDecimalConverter(name, type);
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
            return getVarDecimalConverter(name, type);
          }
          case INTERVAL: {
            IntervalWriter writer = type.isRepetition(Repetition.REPEATED)
                ? getWriter(name, (m, f) -> m.list(f).interval(), l -> l.list().interval())
                : getWriter(name, (m, f) -> m.interval(f), l -> l.interval());
            return new DrillFixedLengthByteArrayToInterval(writer);
          }
          default: {
            VarBinaryWriter writer = type.isRepetition(Repetition.REPEATED)
                ? getWriter(name, (m, f) -> m.list(f).varBinary(), l -> l.list().varBinary())
                : getWriter(name, (m, f) -> m.varBinary(f), l -> l.varBinary());
            return new DrillFixedBinaryToVarbinaryConverter(writer, type.getTypeLength(), mutator.getManagedBuffer());
          }
        }
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type.getPrimitiveTypeName());
    }
  }

  private PrimitiveConverter getVarCharConverter(String name, PrimitiveType type) {
    VarCharWriter writer = type.isRepetition(Repetition.REPEATED)
        ? getWriter(name, (m, f) -> m.list(f).varChar(), l -> l.list().varChar())
        : getWriter(name, (m, f) -> m.varChar(f), l -> l.varChar());
    return new DrillVarCharConverter(writer, mutator.getManagedBuffer());
  }

  private TimeStampWriter getTimeStampWriter(String name, PrimitiveType type) {
    return type.isRepetition(Repetition.REPEATED)
        ? getWriter(name, (m, f) -> m.list(f).timeStamp(), l -> l.list().timeStamp())
        : getWriter(name, (m, f) -> m.timeStamp(f), l -> l.timeStamp());
  }

  private PrimitiveConverter getBigIntConverter(String name, PrimitiveType type) {
    BigIntWriter writer = type.isRepetition(Repetition.REPEATED)
        ? getWriter(name, (m, f) -> m.list(f).bigInt(), l -> l.list().bigInt())
        : getWriter(name, (m, f) -> m.bigInt(f), l -> l.bigInt());
    return new DrillBigIntConverter(writer);
  }

  private PrimitiveConverter getIntConverter(String name, PrimitiveType type) {
    IntWriter writer = type.isRepetition(Repetition.REPEATED)
        ? getWriter(name, (m, f) -> m.list(f).integer(), l -> l.list().integer())
        : getWriter(name, (m, f) -> m.integer(f), l -> l.integer());
    return new DrillIntConverter(writer);
  }

  private PrimitiveConverter getVarDecimalConverter(String name, PrimitiveType type) {
    int s = type.getDecimalMetadata().getScale();
    int p = type.getDecimalMetadata().getPrecision();
    VarDecimalWriter writer = type.isRepetition(Repetition.REPEATED)
        ? getWriter(name, (m, f) -> m.list(f).varDecimal(s, p), l -> l.list().varDecimal(s, p))
        : getWriter(name, (m, f) -> m.varDecimal(f, s, p), l -> l.varDecimal(s, p));
    return new DrillVarDecimalConverter(writer, p, s, mutator.getManagedBuffer());
  }

  @Override
  public Converter getConverter(int i) {
    return converters.get(i);
  }

  @Override
  public void start() {
    if (baseWriter instanceof SingleMapWriter || baseWriter instanceof RepeatedMapWriter) {
      ((MapWriter) baseWriter).start();
    } else {
      ((BaseWriter.ListWriter) baseWriter).startList();
    }
  }

  @Override
  public void end() {
    if (baseWriter instanceof SingleMapWriter || baseWriter instanceof RepeatedMapWriter) {
      ((MapWriter) baseWriter).end();
    } else {
      ((BaseWriter.ListWriter) baseWriter).endList();
    }
  }

  @Override
  public String toString() {
    return converterName;
  }

  private <T> T getWriter(String name, BiFunction<MapWriter, String, T> fromMap, Function<BaseWriter.ListWriter, T> fromList) {
    if (baseWriter instanceof SingleMapWriter || baseWriter instanceof RepeatedMapWriter) {
      return fromMap.apply((MapWriter) baseWriter, name);
    } else if (baseWriter instanceof BaseWriter.ListWriter) {
      return fromList.apply((BaseWriter.ListWriter) baseWriter);
    } else {
      throw new IllegalStateException(String.format("Parent writer with type [%s] is unsupported", baseWriter.getClass()));
    }
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

  /**
   * Converter for field which is present in schema but don't need any actions to be performed by writer.
   * For this purpose the converter is added to converter's chain but simply does nothing and actual writing
   * will be performed by other converters in the chain.
   */
  private static class DrillIntermediateParquetGroupConverter extends DrillParquetGroupConverter {

    DrillIntermediateParquetGroupConverter(OutputMutator mutator, BaseWriter baseWriter, GroupType schema,
                                           Collection<SchemaPath> columns, OptionManager options,
                                           ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates,
                                           boolean skipRepeated, String parentName) {
      super(mutator, baseWriter, schema, columns, options, containsCorruptedDates, skipRepeated, parentName);
    }

    public void start() {}
    public void end() {}
  }


}
