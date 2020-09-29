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
package org.apache.drill.exec.store.avro;

import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.drill.exec.physical.impl.scan.convert.StandardConversions;
import org.apache.drill.exec.physical.impl.scan.v3.FixedReceiver;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.avro.ColumnConverter.ArrayColumnConverter;
import org.apache.drill.exec.store.avro.ColumnConverter.DictColumnConverter;
import org.apache.drill.exec.store.avro.ColumnConverter.DummyColumnConverter;
import org.apache.drill.exec.store.avro.ColumnConverter.MapColumnConverter;
import org.apache.drill.exec.store.avro.ColumnConverter.ScalarColumnConverter;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.DictWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.ValueWriter;
import org.apache.drill.exec.vector.complex.DictVector;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.joda.time.DateTimeConstants;
import org.joda.time.Period;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ColumnConverterFactory {

  private final StandardConversions standardConversions;

  public ColumnConverterFactory(TupleMetadata providedSchema) {
    if (providedSchema == null) {
      standardConversions = null;
    } else {
      standardConversions = StandardConversions.builder().withSchema(providedSchema).build();
    }
  }

  /**
   * Based on given converted Avro schema and current row writer generates list of
   * column converters based on column type.
   *
   * @param readerSchema converted Avro schema
   * @param rowWriter current row writer
   * @return list of column converters
   */
  public List<ColumnConverter> initConverters(TupleMetadata providedSchema,
      TupleMetadata readerSchema, RowSetLoader rowWriter) {
    return IntStream.range(0, readerSchema.size())
      .mapToObj(i -> getConverter(providedSchema, readerSchema.metadata(i), rowWriter.column(i)))
      .collect(Collectors.toList());
  }

  /**
   * Based on column type, creates corresponding column converter
   * which holds conversion logic and appropriate writer to set converted data into.
   * For columns which are not projected, {@link DummyColumnConverter} is used.
   *
   * @param readerSchema column metadata
   * @param writer column writer
   * @return column converter
   */
  public ColumnConverter getConverter(TupleMetadata providedSchema,
      ColumnMetadata readerSchema, ObjectWriter writer) {
    if (!writer.isProjected()) {
      return DummyColumnConverter.INSTANCE;
    }

    if (readerSchema.isArray()) {
      return getArrayConverter(providedSchema,
          readerSchema, writer.array());
    }

    if (readerSchema.isMap()) {
      return getMapConverter(
          providedChildSchema(providedSchema, readerSchema),
          readerSchema.tupleSchema(), writer.tuple());
    }

    if (readerSchema.isDict()) {
      return getDictConverter(
          providedChildSchema(providedSchema, readerSchema),
          readerSchema.tupleSchema(), writer.dict());
    }

    return getScalarConverter(readerSchema, writer.scalar());
  }

  private TupleMetadata providedChildSchema(TupleMetadata providedSchema,
      ColumnMetadata readerSchema) {
    return providedSchema == null ? null :
      providedSchema.metadata(readerSchema.name()).tupleSchema();
  }

  private ColumnConverter getArrayConverter(TupleMetadata providedSchema,
      ColumnMetadata readerSchema, ArrayWriter arrayWriter) {
    ObjectWriter valueWriter = arrayWriter.entry();
    ColumnConverter valueConverter;
    if (readerSchema.isMap()) {
      valueConverter = getMapConverter(providedSchema,
          readerSchema.tupleSchema(), valueWriter.tuple());
    } else if (readerSchema.isDict()) {
      valueConverter = getDictConverter(providedSchema,
          readerSchema.tupleSchema(), valueWriter.dict());
    } else if (readerSchema.isMultiList()) {
      valueConverter = getConverter(null, readerSchema.childSchema(), valueWriter);
    } else {
      valueConverter = getScalarConverter(readerSchema, valueWriter.scalar());
    }
    return new ArrayColumnConverter(arrayWriter, valueConverter);
  }

  private ColumnConverter getMapConverter(TupleMetadata providedSchema,
      TupleMetadata readerSchema, TupleWriter tupleWriter) {
    List<ColumnConverter> converters = IntStream.range(0, readerSchema.size())
      .mapToObj(i -> getConverter(providedSchema, readerSchema.metadata(i), tupleWriter.column(i)))
      .collect(Collectors.toList());
    return new MapColumnConverter(this, providedSchema, tupleWriter, converters);
  }

  private ColumnConverter getDictConverter(TupleMetadata providedSchema,
      TupleMetadata readerSchema, DictWriter dictWriter) {
    ColumnConverter keyConverter = getScalarConverter(
        readerSchema.metadata(DictVector.FIELD_KEY_NAME), dictWriter.keyWriter());
    ColumnConverter valueConverter = getConverter(providedSchema,
        readerSchema.metadata(DictVector.FIELD_VALUE_NAME), dictWriter.valueWriter());
    return new DictColumnConverter(dictWriter, keyConverter, valueConverter);
  }

  private ColumnConverter getScalarConverter(ColumnMetadata readerSchema, ScalarWriter scalarWriter) {
    ValueWriter valueWriter;
    if (standardConversions == null) {
      valueWriter = scalarWriter;
    } else {
      valueWriter = standardConversions.converterFor(scalarWriter, readerSchema);
    }
    return buildScalar(readerSchema, valueWriter);
  }

  public  ScalarColumnConverter buildScalar(ColumnMetadata readerSchema, ValueWriter writer) {
    switch (readerSchema.type()) {
      case VARCHAR:
        return new ScalarColumnConverter(value -> {
          byte[] binary;
          int length;
          if (value instanceof Utf8) {
            Utf8 utf8 = (Utf8) value;
            binary = utf8.getBytes();
            length = utf8.getByteLength();
          } else {
            binary = value.toString().getBytes(Charsets.UTF_8);
            length = binary.length;
          }
          writer.setBytes(binary, length);
        });
      case VARBINARY:
        return new ScalarColumnConverter(value -> {
          if (value instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) value;
            writer.setBytes(buf.array(), buf.remaining());
          } else {
            byte[] bytes = ((GenericFixed) value).bytes();
            writer.setBytes(bytes, bytes.length);
          }
        });
      case VARDECIMAL:
        return new ScalarColumnConverter(value -> {
          BigInteger bigInteger;
          if (value instanceof ByteBuffer) {
            ByteBuffer decBuf = (ByteBuffer) value;
            bigInteger = new BigInteger(decBuf.array());
          } else {
            GenericFixed genericFixed = (GenericFixed) value;
            bigInteger = new BigInteger(genericFixed.bytes());
          }
          BigDecimal decimalValue = new BigDecimal(bigInteger, readerSchema.scale());
          writer.setDecimal(decimalValue);
        });
      case TIMESTAMP:
        return new ScalarColumnConverter(value -> {
          String avroLogicalType = readerSchema.property(AvroSchemaUtil.AVRO_LOGICAL_TYPE_PROPERTY);
          if (AvroSchemaUtil.TIMESTAMP_MILLIS_LOGICAL_TYPE.equals(avroLogicalType)) {
            writer.setLong((long) value);
          } else {
            writer.setLong((long) value / 1000);
          }
        });
      case DATE:
        return new ScalarColumnConverter(value -> writer.setLong((int) value * (long) DateTimeConstants.MILLIS_PER_DAY));
      case TIME:
        return new ScalarColumnConverter(value -> {
          if (value instanceof Long) {
            writer.setInt((int) ((long) value / 1000));
          } else {
            writer.setInt((int) value);
          }
        });
      case INTERVAL:
        return new ScalarColumnConverter(value -> {
          GenericFixed genericFixed = (GenericFixed) value;
          IntBuffer intBuf = ByteBuffer.wrap(genericFixed.bytes())
            .order(ByteOrder.LITTLE_ENDIAN)
            .asIntBuffer();

          Period period = Period.months(intBuf.get(0))
            .withDays(intBuf.get(1)
            ).withMillis(intBuf.get(2));

          writer.setPeriod(period);
        });
      case FLOAT4:
        return new ScalarColumnConverter(value -> writer.setDouble((Float) value));
      case BIT:
        return new ScalarColumnConverter(value -> writer.setBoolean((Boolean) value));
      default:
        return new ScalarColumnConverter(writer::setValue);
    }
  }

  public void buildMapMembers(GenericRecord genericRecord, TupleMetadata providedSchema,
      TupleWriter tupleWriter, List<ColumnConverter> converters) {
    // fill in tuple schema for cases when it contains recursive named record types
    TupleMetadata readerSchema = AvroSchemaUtil.convert(genericRecord.getSchema());
    TupleMetadata tableSchema = FixedReceiver.Builder.mergeSchemas(providedSchema, readerSchema);
    tableSchema.toMetadataList().forEach(tupleWriter::addColumn);

    IntStream.range(0, tableSchema.size())
      .mapToObj(i -> getConverter(providedSchema,
          readerSchema.metadata(i), tupleWriter.column(i)))
      .forEach(converters::add);
  }
}
