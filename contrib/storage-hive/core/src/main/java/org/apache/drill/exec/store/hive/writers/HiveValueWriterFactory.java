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
package org.apache.drill.exec.store.hive.writers;

import java.util.function.BiFunction;
import java.util.function.Function;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.hive.writers.complex.HiveListWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveBinaryWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveBooleanWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveByteWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveCharWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveDateWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveDecimalWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveDoubleWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveFloatWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveIntWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveLongWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveShortWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveStringWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveTimestampWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveVarCharWriter;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.BitWriter;
import org.apache.drill.exec.vector.complex.writer.DateWriter;
import org.apache.drill.exec.vector.complex.writer.Float4Writer;
import org.apache.drill.exec.vector.complex.writer.Float8Writer;
import org.apache.drill.exec.vector.complex.writer.IntWriter;
import org.apache.drill.exec.vector.complex.writer.TimeStampWriter;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.apache.drill.exec.vector.complex.writer.VarDecimalWriter;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
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
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.drill.exec.store.hive.HiveUtilities.throwUnsupportedHiveDataTypeError;

/**
 * Factory used by reader to create Hive writers for columns.
 */
public final class HiveValueWriterFactory {

  private static final Logger logger = LoggerFactory.getLogger(HiveValueWriterFactory.class);

  /**
   * Buffer shared across created Hive writers. May be used by writer for reading data
   * to buffer than from buffer to vector.
   */
  private final DrillBuf drillBuf;

  /**
   * Used to manage and create column writers.
   */
  private final SingleMapWriter rootWriter;

  public HiveValueWriterFactory(DrillBuf drillBuf, SingleMapWriter rootWriter) {
    this.drillBuf = drillBuf;
    this.rootWriter = rootWriter;
  }

  /**
   * Method that will be called once for each column in reader to initialize column writer.
   *
   * @param columnName name of column for writer
   * @param fieldRef   metadata about field type
   * @return instance of writer for column
   */
  public HiveValueWriter createHiveColumnValueWriter(String columnName, StructField fieldRef) {
    ObjectInspector objectInspector = fieldRef.getFieldObjectInspector();
    final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(objectInspector.getTypeName());
    return createHiveValueWriter(columnName, typeInfo, objectInspector, rootWriter);
  }

  private HiveValueWriter createHiveValueWriter(String columnName, TypeInfo typeInfo, ObjectInspector objectInspector, BaseWriter parentWriter) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return createPrimitiveHiveValueWriter(columnName, objectInspector, (PrimitiveTypeInfo) typeInfo, parentWriter);
      case LIST: {
        ListWriter listWriter = extractWriter(columnName, parentWriter, MapWriter::list, ListWriter::list);
        ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
        TypeInfo elemTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
        ObjectInspector elementInspector = listObjectInspector.getListElementObjectInspector();
        HiveValueWriter elementValueWriter = createHiveValueWriter(null, elemTypeInfo, elementInspector, listWriter);
        return new HiveListWriter(listObjectInspector, listWriter, elementValueWriter);
      }
    }
    throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
    return null;
  }

  /**
   * Creates writer for primitive type value.
   *
   * @param name      column name or null if nested
   * @param inspector inspector of column values
   * @param typeInfo  column type used to distinguish returned writers
   * @return appropriate instance of HiveValueWriter for column containing primitive scalar
   */
  private HiveValueWriter createPrimitiveHiveValueWriter(String name, ObjectInspector inspector, PrimitiveTypeInfo typeInfo, BaseWriter parentWriter) {
    switch (typeInfo.getPrimitiveCategory()) {
      case BINARY: {
        VarBinaryWriter writer = extractWriter(name, parentWriter, MapWriter::varBinary, ListWriter::varBinary);
        return new HiveBinaryWriter((BinaryObjectInspector) inspector, writer, drillBuf);
      }
      case BOOLEAN: {
        BitWriter writer = extractWriter(name, parentWriter, MapWriter::bit, ListWriter::bit);
        return new HiveBooleanWriter((BooleanObjectInspector) inspector, writer);
      }
      case BYTE: {
        IntWriter writer = extractWriter(name, parentWriter, MapWriter::integer, ListWriter::integer);
        return new HiveByteWriter((ByteObjectInspector) inspector, writer);
      }
      case DOUBLE: {
        Float8Writer writer = extractWriter(name, parentWriter, MapWriter::float8, ListWriter::float8);
        return new HiveDoubleWriter((DoubleObjectInspector) inspector, writer);
      }
      case FLOAT: {
        Float4Writer writer = extractWriter(name, parentWriter, MapWriter::float4, ListWriter::float4);
        return new HiveFloatWriter((FloatObjectInspector) inspector, writer);
      }
      case INT: {
        IntWriter writer = extractWriter(name, parentWriter, MapWriter::integer, ListWriter::integer);
        return new HiveIntWriter((IntObjectInspector) inspector, writer);
      }
      case LONG: {
        BigIntWriter writer = extractWriter(name, parentWriter, MapWriter::bigInt, ListWriter::bigInt);
        return new HiveLongWriter((LongObjectInspector) inspector, writer);
      }
      case SHORT: {
        IntWriter writer = extractWriter(name, parentWriter, MapWriter::integer, ListWriter::integer);
        return new HiveShortWriter((ShortObjectInspector) inspector, writer);
      }
      case STRING: {
        VarCharWriter writer = extractWriter(name, parentWriter, MapWriter::varChar, ListWriter::varChar);
        return new HiveStringWriter((StringObjectInspector) inspector, writer, drillBuf);
      }
      case VARCHAR: {
        VarCharWriter writer = extractWriter(name, parentWriter, MapWriter::varChar, ListWriter::varChar);
        return new HiveVarCharWriter((HiveVarcharObjectInspector) inspector, writer, drillBuf);
      }
      case TIMESTAMP: {
        TimeStampWriter writer = extractWriter(name, parentWriter, MapWriter::timeStamp, ListWriter::timeStamp);
        return new HiveTimestampWriter((TimestampObjectInspector) inspector, writer);
      }
      case DATE: {
        DateWriter writer = extractWriter(name, parentWriter, MapWriter::date, ListWriter::date);
        return new HiveDateWriter((DateObjectInspector) inspector, writer);
      }
      case CHAR: {
        VarCharWriter writer = extractWriter(name, parentWriter, MapWriter::varChar, ListWriter::varChar);
        return new HiveCharWriter((HiveCharObjectInspector) inspector, writer, drillBuf);
      }
      case DECIMAL: {
        DecimalTypeInfo decimalType = (DecimalTypeInfo) typeInfo;
        int scale = decimalType.getScale();
        int precision = decimalType.getPrecision();
        VarDecimalWriter writer = extractWriter(name, parentWriter,
            (mapWriter, key) -> mapWriter.varDecimal(key, scale, precision),
            listWriter -> listWriter.varDecimal(scale, precision));
        return new HiveDecimalWriter((HiveDecimalObjectInspector) inspector, writer, scale);
      }
      default:
        throw UserException.unsupportedError()
            .message("Unsupported primitive data type %s")
            .build(logger);
    }
  }

  /**
   * Used to extract child writer from parent writer, assuming that parent writer may be instance of
   * {@link MapWriter} or {@link ListWriter}
   *
   * @param name         column or struct field name
   * @param parentWriter parent writer used for getting child writer
   * @param fromMap      function for extracting writer from map parent writer
   * @param fromList     function for extracting writer from list parent writer
   * @param <T>          type of extracted writer
   * @return writer extracted using either fromMap or fromList function
   */
  private static <T> T extractWriter(String name, BaseWriter parentWriter,
                                     BiFunction<MapWriter, String, T> fromMap,
                                     Function<ListWriter, T> fromList) {
    if (parentWriter instanceof MapWriter && name != null) {
      return fromMap.apply((MapWriter) parentWriter, name);
    } else if (parentWriter instanceof ListWriter) {
      return fromList.apply((ListWriter) parentWriter);
    } else {
      throw new IllegalStateException(String.format("Parent writer with type [%s] is unsupported", parentWriter.getClass()));
    }
  }

}
